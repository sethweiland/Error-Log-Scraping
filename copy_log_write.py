import re
import pandas as pd
import requests
import gspread, ast
from oauth2client.client import AccessTokenCredentials, OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client.tools import run_flow, argparser
#from Users\sweiland\Project_calculate_milestone_time\calc_milestone_time import connect_to_google, get_parameters, update_columns
#this function uses oauth2client module in order to connect to google api for google sheets
def connect_to_google():
    #remove sensisitve info from code
    CLIENT_ID='#'
    CLIENT_SECRET='#'
    flow=OAuth2WebServerFlow(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, scope='https://www.googleapis.com/auth/drive', redirect_uri='http://localhost:8080/')
    storage=Storage('#')
    credentials=run_flow(flow, storage, argparser.parse_args([]))

    data = {
        'refresh_token' : credentials.refresh_token,
        'client_id' : credentials.client_id,
        'client_secret' : credentials.client_secret,
        'grant_type' : 'refresh_token',
    }

    r = requests.post('https://accounts.google.com/o/oauth2/token', data = data)
    try:
        credentials.access_token = ast.literal_eval(r.text)['access_token']
    except Exception:
        pass;
    #authorize google authentication credentials for google spreadsheets
    gc = gspread.authorize(credentials)
    return gc





#function to get list that contains all worksheet objects we will write to,and the first row and first column of every sheet
def get_parameters(spreadsheet, worksheets, sheet_list):
    parameters = []
    for i in range(len(sheet_list)):
        worksheets.append(spreadsheet.get_worksheet(i))
    worksheet_one=spreadsheet.get_worksheet(0)
    cell_one=worksheet_one.cell(1, 1)
    row_one=cell_one.row
    column_one=cell_one.col
    parameters.extend((worksheets, row_one, column_one))
    return parameters

#function that does most of the work to update cells within each sheet
def update_columns(worksheet, row, col, columns, execute=True):
    if not columns:
        raise ValueError("Please Specify at least one column")
    #check all columns have same length
    row_len=len(columns[0])
    for column in columns[1:]:
        if len(column) != row_len:
            raise ValueError('not all columns have same length')
    update_cells=[]
    #add columns or rows to current sheet if there are not enough based on lists of lists we are feeding in
    if col + len(columns) > worksheet.col_count:
        worksheet.add_cols(col+len(columns)-worksheet.col_count)
    if row+row_len>worksheet.row_count:
        worksheet.add_rows(row+row_len - worksheet.row_count)

    print("Range %s %s %s %s" % (row, col, row+row_len-1, col + len(columns)-1))
    #get the range on the sheet based on the size(#rows,# columns) of the data we are writing in
    update_range = worksheet.range(row, col, row+row_len-1, col + len(columns)-1)
    print (len(update_range))

    for c, column in enumerate(columns):
        column_range = (update_range[i] for i in range(c, len(update_range), len(columns)))
        #update values in cells based on the data type of given cells and allowing for replacement of old values in cells
        for cell, value in zip(column_range, column):
            if isinstance(value, bool):
                if str(value).upper() != cell.value:
                    cell.value=value
                    update_cells.append(cell)
            elif isinstance(value, (int, float)):
                if cell.numeric_value is None:
                    cell.value = value
                    update_cells.append(cell)
            elif isinstance(value, str):
                if value != cell.value:
                    cell.value = value
                    update_cells.append(cell)
            elif value is None:
                if '' != cell.value:
                    cell.value=''
                    update_cells.append(cell)
            else:
                raise ValueError("Cell value {} Must be of type string , number, or boolean. Not {}".format(value, type(value)))
    #execeute set to true in our parameters for function, tells us how many cells are being updated and then updates the cells in current worksheet object
    if execute:
        print("Updating %d cells." % len(update_cells))
        if update_cells:
            worksheet.update_cells(update_cells)
        return len(update_cells)
    else:
        return update_cells
#list of txt log files we will loop through and create lsit of lists for each text file that mimic a datatable/pandas dataframe
list_of_files = ['robo_log_10_1_2018.txt', 'robo_log_10_2_2018.txt', 'robo_log_10_3_2018.txt', 'robo_log_10_4_2018.txt', 'robo_log_10_5_2018.txt', 'robo_log_10_8_2018.txt', 'robo_log_10_9_2018.txt']
#final list of list of lists that will hold multiple list of lists with each list of list representing a data table/future google sheet.
list_of_list_of_lists = []
#begin to loop through text files and create our list of lists
for file in list_of_files:
    with open (file, 'r') as myfile:
        data = myfile.read()
    #regex to pull out loan number from log for each error
    loan_id = re.compile('\d{10}')
    #reg ex to pull out two different kinds of error patterns that are common in these logs
    error = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}  \[Error\] : .+ |\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}  \[Error-Ex\] : .+')
    #each entry for loan being tested by bot was seperated by a long list of asterisk characters(*) which is why I split up log as one big string into smaller strings seperated by multiple *
    list_data = data.split('**************************************************************************************************************')
    #create empty lists that will each hold a column of a future google sheet with loan id, error text, and timestamp of error occurence
    list_of_errors = []
    list_of_id = []
    list_of_error_date = []
    for item in list_data[1:]:
        match_id = re.search(loan_id, item)
        loan_num = match_id.group(0)
        match_error = re.findall(error, item)
        for index, item in enumerate(match_error):
            #print(loan_num +' ' + match_error[index])
            error_date = match_error[index].split('[')[0]
            error_instance = '[' + match_error[index].split('[')[1]
            list_of_error_date.append(error_date)
            list_of_errors.append(error_instance)
            list_of_id.append(loan_num)
    list_of_error_date.insert(0, 'Timestamp')
    list_of_errors.insert(0, 'Error_Message')
    list_of_id.insert(0, 'Loan_ID')
    list_of_list = []
    list_of_list.append(list_of_error_date)
    list_of_list.append(list_of_id)
    list_of_list.append(list_of_errors)
    list_of_list_of_lists.append(list_of_list)
    print(len(list_of_list))
    print(len(list_of_error_date))
    print(len(list_of_id))
    print(len(list_of_errors))

    
    

#write to a google sheets spreadsheet
sheet_list = ['10-01-2018', '10-02-2018', '10-03-2018', '10-04-2018', '10-05-2018', '10-08-2018', '10-09-2018']
    # create a gspread spreadsheet object from a URL, we can also use sheet_id or sheet name.
gc = connect_to_google()
spreadsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1CRo0hVbrYQ9tU0RhO7Rc-gjvmeaufvWxrM02TsFgZ0Y/edit#gid=0')

    # worksheets_test will be a list of worksheet objects that will iterate over later during writing
worksheets = []
parameters=get_parameters(spreadsheet, worksheets, sheet_list)

for index, item in enumerate(parameters[0]):
    update_columns(item, parameters[1], parameters[2], list_of_list_of_lists[index], execute=True)
