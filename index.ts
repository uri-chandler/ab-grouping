import { parse as csvParse } from "csv-parse/sync";
import { parse as jsonParse } from 'json2csv';
import * as fs from 'fs';
import * as Toolog from 'toolog'

const log = new Toolog('ab-grouping');

interface CSVRow {
    column_a:string;
    column_b:string;
}

async function start(){
    assert_input_file_exists();
    const input_data = read_input_file();
    const aggregated_data = group_data_by_first_column(input_data);

    count_second_column_per_group(aggregated_data);
    pick_highest_recurring_count(aggregated_data);

    const final_data_for_output = restructure_for_output_file(aggregated_data);

    write_output_file(final_data_for_output);

    log.done('All done! \n');
}

function write_output_file(data) {
    log.info('Writing final output file to output.csv');

    fs.writeFileSync(
        'output.csv',

        jsonParse(
            data,
            {
                fields: [
                    'column_a',
                    'column_b',
                    'count'
                ]
            }
        ),

        'utf-8'
    )

    log.done(' -> ok \n');
}

function restructure_for_output_file(data:{ [key:string]: Object }) {
    log.info('Preparing data for output.csv file')

    const restructured = Object.entries(data).map(x => ({
        column_a: x[0],
        column_b: x[1][0],
        count: x[1][1]
    }));

    log.done(' -> ok \n');
    return restructured;
}

function pick_highest_recurring_count(data:{ [key:string]: Object }) {
    log.info('Sorting by highest recurring count of Column B');

    for (const key in data) {
        data[key] = Object.entries(data[key])
                          .sort((a, b) => a[1] > b[1] ? 1 : -1)
                          .pop();
    }

    log.done(' -> ok \n');
}

function count_second_column_per_group(data:{ [key:string]: Array<string> }) {
    log.info('Counting data by Column B');

    for (const key in data) {
        (data[key] as Object) = data[key].reduce((all, one) => {
            all[one] = all[one] || 0;
            all[one] += 1;
            return all;
        }, {});
    }

    log.done(' -> ok \n');
}

function group_data_by_first_column(data:Array<CSVRow>) {
    /**
     * From this:
     *   [
     *      {
     *          column_a: "Company One",
     *          column_b: "domain-one.com"
     *      },
     *
     *      {
     *          column_a: "Company One",
     *          column_b: "domain-two.com"
     *      },
     *
     *      {
     *          column_a: "Company Two",
     *          column_b: "domain-two.com"
     *      }
     *   ]
     *
     * To this:
     *   {
     *      "Company One": [
     *          "domain-one.com",
     *          "domain-two.com",
     *      ],
     *
     *      "Company Two": [
     *          "domain-two.com",
     *      ]
     *   }
     */
    log.info('Grouping data by Column A');
    const grouped_data = data.reduce((group, row) => {
        group[row.column_a] = group[row.column_a] || [];
        group[row.column_a].push(row.column_b);
        return group;
    }, {});

    log.done(' -> ok \n');
    return grouped_data;
}

function read_input_file() {
    log.info('Reading input file');

    try {
        const parsed_data = csvParse(
            fs.readFileSync('input.csv', 'utf-8'),

            {
                columns:[
                    'column_a',
                    'column_b'
                ],

                from: 2
            },
        );

        log.done(' -> ok\n');
        return parsed_data;
    }
    catch(err) {
        exit_with_warning('Could not read from input file. Maybe the file is not structured properly?');
    }
}

function assert_input_file_exists() {
    log.info('Looking for file input.csv');

    const does_file_exist = fs.existsSync('input.csv')

    if (does_file_exist === false)
        exit_with_warning('Input file was not found. Did you add an "input.csv" file to this folder?');
    else
        log.done(' -> ok\n');
}

function exit_with_warning(msg) {
    log.warn(msg + '\n');
    process.exit();
}


start().catch(console.error);