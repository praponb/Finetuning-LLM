WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        k,
        attr_value,
        CAST(log_ts AS DATE) log_ts_day,
        COALESCE(k, 'OTHER') k_clean
    FROM lines
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        latency_ms,
        k,
        attr_value,
        log_ts_day,
        LAST_DAY(log_ts_day) log_ts_day_month_end,
        CASE WHEN latency_ms >= 2478 THEN 'High' WHEN latency_ms >= 1154 THEN 'Medium' ELSE 'Low' END latency_ms_tier
    FROM cleaned
    WHERE k IS NOT DISTINCT FROM 'region'
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(log_ts_day, 'yyyy-MM') log_ts_day_label,
        COALESCE(k, 'unassigned') k_filled,
        COALESCE(k, 'UNKNOWN') k_filled_2
    FROM filtered
    WHERE YEAR(log_ts_day) = 2024
)
SELECT
    service,
    k,
    attr_value,
    log_ts_day,
    log_ts_day_month_end,
    latency_ms_tier,
    log_ts_day_label,
    k_filled,
    k_filled_2
FROM filtered_app_logs
