import csv
import os
from flask import Flask, make_response, jsonify
from google.cloud import logging, storage
from datetime import datetime, date

app = Flask(__name__)

logging_client = logging.Client()

FILTER = 'protoPayload.methodName="google.cloud.dialogflow.v2.Sessions.DetectIntent" ' \
         'OR resource.labels.service_name="honda-va-poc-dev-env"'

bucket_name = os.getenv('BUCKET')


@app.route('/logging/', methods=['POST'])
def logging_function(request):
    UUID_list = []
    log_list = []
    intent_needed = False
    for entry in logging_client.list_entries(filter_=FILTER, page_size=1000):
        if entry.payload is not None and type(entry.payload) == str:
            if entry.payload.startswith("H__"):
                if not entry.payload.startswith("H__Console__"):
                    timestamp = entry.timestamp
                    payload = entry.payload.split("__")
                    UUID = payload[1]
                    if payload[2] == "SA":
                        if entry.payload.split("__")[3] == 'start_conversation':
                            operation = "Start conversation marker"
                        elif entry.payload.split("__")[3] == 'end_conversation':
                            operation = "End conversation marker"
                            time_taken = entry.payload.split("__")[5][:-3]

                    elif payload[2] == "FF":
                        if 'Start' in entry.payload.split("__")[3]:
                            operation = "Fulfilment Start"
                        elif 'End' in entry.payload.split("__")[3]:
                            operation = "Fulfilment End"

                    elif payload[2] == "DB":
                        if 'Start' in entry.payload.split("__")[3]:
                            operation = "Database Start_0"
                        elif 'End' in entry.payload.split("__")[3]:
                            operation = "Database End_0"
                    else:
                        continue
                    if UUID not in UUID_list:
                        UUID_list.append(UUID)
                        if "Start conversation" in operation:
                            intent_needed = True
                        dict1 = {"UUID": UUID, operation: timestamp.isoformat()}
                        log_list.append(dict1)
                    else:
                        ind = UUID_list.index(UUID)
                        if "Database" in operation:
                            if log_list[ind].__contains__(operation):
                                operation = operation[:-1] + str(int(operation[-1]) + 1)
                        elif "End conversation" in operation:
                            log_list[ind]["stubApp Reported"] = time_taken
                        elif "Start conversation" in operation:
                            intent_needed = True
                        log_list[ind][operation] = timestamp.isoformat()
        elif entry.payload is not None and type(entry.payload) == dict:
            if entry.payload.__contains__("methodName"):
                if entry.payload['methodName'] == 'google.cloud.dialogflow.v2.Sessions.DetectIntent':
                    if intent_needed == True:
                        UUID = UUID_list[-1]
                        ind = UUID_list.index(UUID)
                        log_list[ind]["Detect Intent"] = entry.timestamp.isoformat()
                        intent_needed = False

    for ele in log_list:
        if ele.__contains__("Detect Intent") and ele.__contains__("Fulfilment Start"):
            ele["detect intent time"] = (datetime.fromisoformat(ele["Fulfilment Start"]) - datetime.fromisoformat(
                ele["Detect Intent"])).microseconds // 1000

        if ele.__contains__("Fulfilment End") and ele.__contains__("Fulfilment Start"):
            ele["fulfilment time"] = (datetime.fromisoformat(ele["Fulfilment End"]) - datetime.fromisoformat(
                ele["Fulfilment Start"])).microseconds // 1000

        if ele.__contains__("fulfilment time") and ele.__contains__("stubApp Reported") and ele.__contains__("detect "
                                                                                                             "intent "
                                                                                                             "time"):
            ele["Network latency"] = int(ele['stubApp Reported']) - int(ele["fulfilment time"]) - \
                                     int(ele["detect intent time"])

        new_list1 = []
        new_list2 = []
        for i in ele.keys():
            if i.startswith('Database Start'):
                new_list1.append(datetime.fromisoformat(ele[i]))
            if i.startswith('Database End'):
                new_list2.append(datetime.fromisoformat(ele[i]))
        delta = 0

        if len(new_list1) != 0:
            for j in range(0, len(new_list1)):
                delta += (new_list2[j] - new_list1[j]).microseconds // 1000
        if delta != 0:
            ele["Database time"] = delta
    date_param = date.today()
    f = open(f"/tmp/hud_ref_perf_{date_param}.csv", "w")
    # f = open("hud_ref_perf.csv", "w")
    writer = csv.DictWriter(
        f, fieldnames=['UUID', 'Start conversation marker', 'Detect Intent', 'Fulfilment Start',
                       'Database Start_0', 'Database End_0', 'Database Start_1',
                       'Database End_1', 'Database Start_2', 'Database End_2',
                       'Fulfilment End', 'End conversation marker', 'stubApp Reported',  "detect intent time",
                       "fulfilment time", "Database time", 'Network latency'])

    writer.writeheader()
    writer.writerows(log_list)
    f.close()
    # return bytes(csv, encoding='UTF-8'), 200, {'Content-Type': 'text/csv',
    #                                            'Content-Disposition': 'attachment; filename="hud_ref_perf.csv"'}
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(f"hud_ref_perf_{date_param}.csv")
    blob.upload_from_filename(f"/tmp/hud_ref_perf_{date_param}.csv")

    return make_response(jsonify({'message': "csv file generated"}), 200)


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    print("Starting app on port %d" % port)
    app.run(debug=False, port=port, host='0.0.0.0')
