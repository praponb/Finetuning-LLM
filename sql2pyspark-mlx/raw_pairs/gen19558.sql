WITH lines AS (SELECT id, host, level, attr_key, v FROM app_logs LATERAL VIEW EXPLODE(context) t AS attr_key, v) SELECT id, level, attr_key FROM lines;
