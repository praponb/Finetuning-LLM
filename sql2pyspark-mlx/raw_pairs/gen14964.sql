WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, attr_value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        level,
        status_code,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM exploded
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM flattened_attrs
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        key,
        status_code_high,
        SUM(new_group_flag) OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
islands_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        key,
        status_code_high,
        SUM(session_start_flag) OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        level,
        key,
        status_code_high
    FROM islands_app_logs
) src
PIVOT (
    MAX(status_code_high)
    FOR key IN ('trace', 'region')
)
