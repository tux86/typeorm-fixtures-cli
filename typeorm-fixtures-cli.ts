/***
 * typeorm-cli very fast fixtures loading (based on transactions)
 *
 * Usage example :
 * ts-node typeorm-fixtures-cli.ts --fixturesPath ./fixtures --transactionBatchSize 250 --delete
 */
import * as path from 'path'
import * as fs from 'fs'
import { program } from 'commander'
import { Builder, fixturesIterator, IEntity, Loader, Parser, Resolver } from 'typeorm-fixtures-cli'
import { getConnection } from '@/clients/typeOrm'
import { getRepository } from 'typeorm'

interface Options {
    fixturesPath: string,
    transactionBatchSize: number
    deleteEnabled: boolean
}

program.version('0.0.1')
  .requiredOption('-p, --fixturesPath <path>', 'fixtures directory path', './fixtures')
  .requiredOption('-s, --transactionBatchSize <size>', 'transaction batch size', '250')
  .option('-d, --delete', 'enable data deleting before loading fixtures')

program.parse(process.argv)

const opts = program.opts()

const options = {
  fixturesPath: opts.fixturesPath,
  transactionBatchSize: parseInt(opts.transactionBatchSize),
  deleteEnabled: opts.delete || false
}

const loadFixtures = async (options: Options) => {
  const { fixturesPath, transactionBatchSize, deleteEnabled } = options

  const connection = await getConnection()
  await connection.synchronize(false)
  const queryRunner = connection.createQueryRunner()
  try {
    const loader = new Loader()

    // load fixtures files
    fs.readdirSync(fixturesPath).forEach(file => {
      const fullPath = path.resolve(fixturesPath, file)
      if (fs.lstatSync(fullPath).isDirectory()) {
        loader.load(fullPath)
      }
    })

    const resolver = new Resolver()
    const fixtures = resolver.resolve(loader.fixtureConfigs)
    const builder = new Builder(connection, new Parser())

    let total = 0
    const entitiesMap = new Map<string, IEntity[]>()
    const entityTableMapping = new Map<string, string>()
    for (const fixture of fixturesIterator(fixtures)) {
      total++
      const entity = await builder.build(fixture)
      const { name } = entity.constructor
      if (!entityTableMapping.has(name)) {
        entityTableMapping.set(name, getRepository(name).metadata.tableName)
      }
      if (!entitiesMap.has(name)) {
        entitiesMap.set(name, [])
      }
      entitiesMap.get(name).push(entity)
    }

    console.log('total fixtures:', total)
    // =================== WRITE TRANSACTION ========================

    await queryRunner.startTransaction()
    const truncatePromises = []
    if (deleteEnabled) {
      for (const [k, v] of entityTableMapping) {
        // console.log('truncate ', v)
        truncatePromises.push(queryRunner.query(`TRUNCATE TABLE ${v} CASCADE`))
      }
    }
    process.stdout.write('deleting ... ')
    await Promise.all(truncatePromises)
    process.stdout.write('ok\n')

    for (const [entityName, entities] of entitiesMap) {
      const total = entities.length
      const savePromises = []
      while (entities.length > 0) {
        const entitiesBatch = entities.splice(0, transactionBatchSize)
        savePromises.push(queryRunner.manager.save(entityName, entitiesBatch))
      }
      process.stdout.write(`persisting "${entityName}" ${total} fixture(s) ... `)
      await Promise.all(savePromises)
      process.stdout.write('ok\n')
    }
    await queryRunner.commitTransaction()
  } catch (err) {
    console.error(err)
    if (queryRunner.isTransactionActive) {
      await queryRunner.rollbackTransaction()
    }
    throw err
  } finally {
    await queryRunner.release()
    if (connection) {
      await connection.close()
    }
  }
}
loadFixtures(options)
  .then(() => {
    console.log('Fixtures are successfully loaded.')
  })
  .catch(err => console.log(err))
