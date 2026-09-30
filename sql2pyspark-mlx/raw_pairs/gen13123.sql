WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        attr_value,
        AVG(latency_ms) OVER (PARTITION BY YEAR(log_ts), service ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_5_prior,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host, service), 4) AS latency_ms_share,
        FIRST_VALUE(latency_ms) IGNORE NULLS OVER (ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms
    FROM lines
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        latency_ms_avg_5_prior,
        SUM(is_new_burst) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY log_ts DESC, id ASC, attr_key ASC) AS rn
        FROM islands
    ) ranked
    WHERE rn = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        latency_ms_avg_5_prior,
        session_no,
        rn,
        session_start_flag,
        YEAR(log_ts) AS log_ts_year
    FROM sessions_app_logs
    WHERE log_ts >= TIMESTAMP '2025-06-01 14:00:00'
      AND attr_key IS NOT NULL
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC) AS prev_log_ts
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY attr_key ORDER BY log_ts ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    attr_key,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN attr_key = 'version' THEN 1 END), 0), 2) AS session_no_per_version,
    SUM(CASE WHEN attr_key = 'region' THEN latency_ms_avg_5_prior ELSE 0 END) AS region_latency_ms_avg_5_prior
FROM islands_app_logs
GROUP BY attr_key
