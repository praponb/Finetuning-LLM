WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-02'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(kwh_bucket) OVER (PARTITION BY quality), 2) AS quality_avg_kwh_bucket,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        LEAD(kwh_bucket, 2) OVER (PARTITION BY quality, site_id ORDER BY voltage NULLS LAST, id) AS next2_kwh_bucket
    FROM recent_readings
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE rows_in_quality < (SELECT MAX(rows_in_quality) FROM ranked)
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        quality,
        ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio,
        SUM(CASE WHEN quality = 'EST' THEN 1 ELSE 0 END) AS n_quality_est,
        ROUND(SUM(voltage), 2) AS total_voltage,
        COUNT(DISTINCT quality) AS distinct_qualitys
    FROM filtered
    GROUP BY DATE_TRUNC('MONTH', read_at), quality
    HAVING SUM(rows_in_quality) > 6
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE total_voltage < (SELECT MAX(total_voltage) FROM grouped)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        read_at_month,
        quality,
        voltage_to_kwh_ratio,
        n_quality_est,
        total_voltage,
        distinct_qualitys,
        ROW_NUMBER() OVER (ORDER BY total_voltage DESC NULLS FIRST, read_at_month, quality) AS total_voltage_seq,
        SUM(voltage_to_kwh_ratio) OVER (ORDER BY total_voltage DESC NULLS FIRST, read_at_month, quality ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage_to_kwh_ratio,
        SUM(total_voltage) OVER (PARTITION BY n_quality_est) AS n_quality_est_sum_total_voltage
    FROM with_activity
)
SELECT
    read_at_month,
    quality,
    voltage_to_kwh_ratio,
    total_voltage,
    distinct_qualitys,
    total_voltage_seq,
    running_voltage_to_kwh_ratio,
    n_quality_est_sum_total_voltage,
    read_at_month + INTERVAL 15 MINUTES AS read_at_month_plus_15m,
    FLOOR(n_quality_est / 2) * 2 AS n_quality_est_bucket
FROM metrics
WHERE HOUR(read_at_month) BETWEEN 6 AND 10
ORDER BY n_quality_est_bucket DESC, n_quality_est_sum_total_voltage
