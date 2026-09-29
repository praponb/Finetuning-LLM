WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM exploded
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        SUM(new_group_flag) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM islands
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        session_id,
        SUM(session_start_flag) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, k ORDER BY log_ts DESC, id, k) AS row_num
    FROM sessionized
    QUALIFY row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        session_id,
        row_num,
        LEAD(session_id) OVER (PARTITION BY level ORDER BY log_ts, id, k) AS next_session_id,
        LEAD(session_id_2) OVER (PARTITION BY level ORDER BY log_ts, id, k) AS next_session_id_2,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts, id, k) AS seq_no
    FROM latest
)
SELECT
    id,
    level,
    latency_ms,
    k,
    session_id,
    next_session_id_2,
    seq_no
FROM enriched
ORDER BY next_session_id_2 DESC NULLS FIRST;
