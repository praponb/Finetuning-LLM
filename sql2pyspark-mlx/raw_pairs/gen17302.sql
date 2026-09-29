WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(level) AS clean_level
    FROM exploded
    WHERE log_ts >= DATE '2025-06-01'
      AND key IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY level, key ORDER BY new_group_flag DESC NULLS LAST) AS dedup_rank
    FROM sessions_app_logs
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(latency_ms) OVER (PARTITION BY level, key ORDER BY new_group_flag, id, key) AS prev_latency_ms,
        LEAD(latency_ms) OVER (PARTITION BY level, key ORDER BY new_group_flag, id, key) AS next_latency_ms,
        LEAD(session_id) OVER (PARTITION BY level, key ORDER BY new_group_flag, id, key) AS next_session_id,
        ROW_NUMBER() OVER (PARTITION BY level, key ORDER BY new_group_flag, id, key) AS event_seq
    FROM latest
)
-- Aggregate with conditional metrics
SELECT
    next_session_id,
    MIN(log_ts) AS first_seen,
    BIT_AND(latency_ms) AS latency_ms_bit_and
FROM enriched
GROUP BY next_session_id
ORDER BY latency_ms_bit_and DESC NULLS LAST;
