WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, v
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM lines
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        log_ts,
        k,
        status_code_high,
        is_flagged
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k, level ORDER BY log_ts DESC, id ASC, k ASC) AS rank_in_group
        FROM flattened_attrs
    ) ranked
    WHERE rank_in_group = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts ASC, id ASC, k ASC) AS prev_log_ts
    FROM ranked
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    id,
    k,
    status_code_high,
    prev_log_ts,
    is_new_session
FROM sessions_app_logs
ORDER BY is_new_session ASC NULLS FIRST
