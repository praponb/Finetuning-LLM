WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE log_ts < '2025-09-19'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), log_ts) / 365 years_since_log_ts
    FROM unpacked
    WHERE status_code_high < 2
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, service
    FROM cleaned
    EXCEPT ALL
    SELECT id, service
    FROM app_logs
    WHERE latency_ms <= 2252
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.id = k.id AND s.service = k.service
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(status_code_high) OVER (PARTITION BY service) service_count_status_code_high,
        AVG(years_since_log_ts) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) years_since_log_ts_avg_2h,
        MAX(status_code_high) OVER (PARTITION BY YEAR(log_ts), service) log_ts_year_max_status_code_high,
        AVG(years_since_log_ts) OVER (PARTITION BY YEAR(log_ts) ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) years_since_log_ts_avg_1h,
        CUME_DIST() OVER (PARTITION BY service ORDER BY status_code_high DESC NULLS LAST) status_code_high_cume_dist
    FROM verified
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN years_since_log_ts >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS years_since_log_ts_tier,
        DATE_TRUNC('DAY', log_ts) AS activity_day,
        SIZE(COLLECT_SET(service)) AS num_services,
        SIZE(COLLECT_SET(service)) AS num_services_2,
        SORT_ARRAY(COLLECT_SET(service)) AS service_list
    FROM enriched
    GROUP BY CASE WHEN years_since_log_ts >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('DAY', log_ts)
    HAVING SIZE(COLLECT_SET(service)) >= 1
)
SELECT
    years_since_log_ts_tier,
    activity_day,
    num_services
FROM grouped
ORDER BY years_since_log_ts_tier ASC;
