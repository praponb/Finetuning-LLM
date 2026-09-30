SELECT id, host, level, context, CAST((status_code & 4) != 0 AS INT) AS flag_bit4, GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text FROM app_logs WHERE level = 'DEBUG'
