WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value,
        STDDEV_SAMP(status_code) OVER (PARTITION BY service) AS service_stddev_samp_status_code,
        SUM(status_code) OVER (PARTITION BY attr_key, level ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
        FIRST_VALUE(attr_key) IGNORE NULLS OVER (ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key
    FROM flattened
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cumulative_status_code) OVER (PARTITION BY level ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_status_code,
        ROUND(service_stddev_samp_status_code / SUM(service_stddev_samp_status_code) OVER (PARTITION BY level), 4) AS service_stddev_samp_status_code_share,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY status_code DESC) AS status_code_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(log_ts), service ORDER BY log_ts DESC, id, attr_key) AS seq_no,
        ROUND(service_stddev_samp_status_code / SUM(service_stddev_samp_status_code) OVER (PARTITION BY service), 4) AS service_stddev_samp_status_code_share_2
    FROM metrics
    WHERE service = 'auth'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(seq_no / cumulative_status_code, 2) AS seq_no_per_cumulative_status_code,
        TO_DATE(log_ts) AS log_ts_date,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM with_windows
    WHERE attr_key IS NOT NULL
      AND attr_key NOT IN ('region', 'trace')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value,
        first_known_attr_key,
        service_stddev_samp_status_code_share,
        SUM(session_start_flag) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_tier,
    level,
    CASE WHEN service_stddev_samp_status_code_share >= 0.69 THEN 'HIGH' ELSE 'LOW' END AS service_stddev_samp_status_code_share_tier,
    SIZE(COLLECT_SET(level)) AS num_levels,
    SUM(CASE WHEN attr_key = 'trace' THEN status_code ELSE 0 END) AS trace_status_code,
    MAX(log_ts) AS last_log_ts,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SIZE(COLLECT_SET(level)) AS num_levels_2,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN session_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN attr_key = 'trace' THEN status_code ELSE 0 END) DESC) AS trace_status_code_percent_rank
FROM sessions_app_logs
GROUP BY session_id_tier, level, service_stddev_samp_status_code_share_tier;
