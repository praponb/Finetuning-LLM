WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code) OVER w_grp AS status_code_sum_2h,
        SUM(status_code) OVER (ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code,
        NTILE(5) OVER (ORDER BY status_code, id, key) AS status_code_bucket
    FROM lines
    WINDOW w_grp AS (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    service,
    log_ts,
    status_code,
    key,
    value,
    status_code_sum_2h,
    status_code_bucket,
    session_id,
    CASE WHEN key RLIKE '^(trace|region)$' THEN 'CORE' ELSE 'GROUP_A' END AS key_zone
FROM sessionized
