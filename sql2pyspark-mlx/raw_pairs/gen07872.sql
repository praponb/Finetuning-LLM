WITH exploded AS (SELECT id, service, log_ts, level, status_code, k, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) li AS k, v) SELECT service, log_ts, v FROM exploded ORDER BY log_ts
