WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(latency_ms) OVER (PARTITION BY host ORDER BY status_code, id, attr_key) AS next_latency_ms,
        LAG(latency_ms) OVER (PARTITION BY host ORDER BY status_code, id, attr_key) AS prev_latency_ms,
        LEAD(status_code) OVER (PARTITION BY host ORDER BY status_code, id, attr_key) AS next_status_code,
        LAG(status_code) OVER (PARTITION BY host ORDER BY status_code, id, attr_key) AS prev_status_code,
        ROW_NUMBER() OVER (PARTITION BY host ORDER BY status_code, id, attr_key) AS row_seq
    FROM lines
    WHERE latency_ms > 1944
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, attr_key) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY level ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value,
        next_latency_ms,
        prev_latency_ms,
        prev_status_code,
        is_new_burst,
        session_id,
        COUNT(is_new_burst) OVER w_running AS last7_count_is_new_burst,
        MIN(is_new_burst) OVER w_running AS last7_min_is_new_burst,
        SUM(is_new_burst) OVER w_running AS last7_sum_is_new_burst,
        AVG(row_seq) OVER w_hist AS row_seq_avg_3_prior
    FROM sessionized
    WINDOW w_running AS (PARTITION BY host ORDER BY status_code, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY row_seq, id, attr_key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
summary AS (
    -- Grouped metrics
    SELECT
        attr_key,
        COUNT(*) FILTER (WHERE attr_key = 'trace') AS trace_rows,
        SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
        SIZE(COLLECT_SET(level)) AS num_levels,
        PERCENTILE_APPROX(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles
    FROM metrics
    GROUP BY attr_key
)
SELECT
    attr_key,
    num_levels,
    CASE WHEN attr_key RLIKE '^(trace|region)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS attr_key_zone
FROM summary
ORDER BY num_levels DESC, attr_key DESC
LIMIT 5;
