WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        kwh - voltage AS kwh_minus_voltage,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-18'
),
grouped AS (
    -- Group-level totals
    SELECT
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_id,
        ROUND(STDDEV(voltage_bucket), 2) AS stddev_voltage_bucket,
        SUM(CASE WHEN quality = 'BAD' THEN 1 ELSE 0 END) AS n_quality_bad,
        MAX(kwh_minus_voltage) AS max_kwh_minus_voltage,
        MIN(read_at) AS first_read_at,
        ROUND(100 * COUNT(CASE WHEN quality IN ('OK', 'EST') THEN 1 END) / COUNT(*), 1) AS pct_quality_in_ok_est
    FROM base
    GROUP BY quality, DATE_TRUNC('MONTH', read_at), site_id
),
joined_site AS (
    -- Join sites
    SELECT s.*, si.site_name, si.installed_kw AS site_installed_kw
    FROM grouped s
    JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT first_read_at, quality, stddev_voltage_bucket
    FROM joined_site
) src
PIVOT (
    SUM(stddev_voltage_bucket)
    FOR quality IN ('BAD', 'EST')
)
ORDER BY first_read_at
