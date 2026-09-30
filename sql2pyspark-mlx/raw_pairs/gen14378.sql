WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
)
SELECT
    *,
    LAST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY host, attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
    FIRST_VALUE(attr_key, TRUE) OVER w_grp AS first_known_attr_key,
    LAST_VALUE(attr_key, TRUE) OVER w_grp AS last_known_attr_key_2,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', log_ts), attr_key ORDER BY log_ts DESC, id, attr_key) AS seq_no
FROM lines
WHERE attr_key = 'region'
WINDOW w_grp AS (ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
