WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
)
SELECT
    *,
    COUNT(status_code) OVER (PARTITION BY DATE_TRUNC('WEEK', log_ts), attr_key) AS log_ts_period_count_status_code,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share
FROM lines;
