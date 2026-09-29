WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
)
SELECT
    id,
    log_ts,
    level,
    status_code,
    attr_key,
    AVG(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY MONTH(log_ts), service), 4) AS status_code_share,
    SUM(status_code) OVER (ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
    NTILE(3) OVER (PARTITION BY YEAR(log_ts), service ORDER BY status_code DESC, id, attr_key) AS status_code_bucket,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code, id, attr_key) AS seq_no
FROM exploded
WHERE status_code = 334;
