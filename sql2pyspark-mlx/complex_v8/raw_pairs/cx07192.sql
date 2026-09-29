WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
),
grouped AS (
    -- Group-level totals
    SELECT
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        COUNT(*) AS n_rows,
        MIN(read_at) AS first_read_at,
        ROUND(SUM(voltage), 2) AS total_voltage,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM scoped
    GROUP BY 1, 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(total_voltage - AVG(total_voltage) OVER (PARTITION BY MONTH(first_read_at), quality), 2) AS total_voltage_vs_first_read_at_month_quality_avg,
        ROW_NUMBER() OVER (PARTITION BY n_rows ORDER BY n_rows, quality, read_at_ym) AS rn
    FROM grouped
)
-- Final projection
SELECT
    quality,
    read_at_ym,
    n_rows,
    total_voltage,
    quality_list,
    total_voltage_vs_first_read_at_month_quality_avg,
    rn,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
    ABS(rn - 5) AS rn_distance
FROM metrics
ORDER BY read_at_ym DESC
