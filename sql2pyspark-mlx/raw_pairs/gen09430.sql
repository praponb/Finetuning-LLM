WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
kept_keys AS (
    -- Set difference on keys
    SELECT service, level
    FROM lines
    EXCEPT ALL
    SELECT service, level
    FROM app_logs
    WHERE status_code >= 395
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.service = k.service AND s.level = k.level
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_2h
    FROM kept_rows
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v,
        status_code_max_2h,
        latency_ms - status_code_max_2h AS latency_ms_minus_status_code_max_2h
    FROM scored
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND attr_key IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    attr_key,
    v,
    latency_ms_minus_status_code_max_2h,
    ROUND(status_code_max_2h / SUM(status_code_max_2h) OVER (PARTITION BY attr_key), 4) AS status_code_max_2h_share,
    CORR(status_code, latency_ms) OVER (PARTITION BY level) AS corr_status_code_latency_ms,
    PERCENT_RANK() OVER (PARTITION BY YEAR(log_ts) ORDER BY latency_ms_minus_status_code_max_2h DESC) AS latency_ms_minus_status_code_max_2h_percent_rank,
    AVG(latency_ms_minus_status_code_max_2h) OVER (PARTITION BY DATE_TRUNC('week', log_ts), level) AS log_ts_period_avg_latency_ms_minus_status_code_max_2h,
    AVG(latency_ms_minus_status_code_max_2h) OVER (PARTITION BY MONTH(log_ts), level) AS log_ts_month_avg_latency_ms_minus_status_code_max_2h
FROM filtered
