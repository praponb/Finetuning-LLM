WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE (voltage < 234.5 OR MONTH(read_at) IN (2, 3))
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        MONTH(read_at) AS read_at_month,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM valid
    WHERE voltage BETWEEN 213.7 AND 235.0
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh >= 42.1
              AND m.read_at < TIMESTAMP '2025-02-18 10:00:00'
        )
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on < DATE '2025-05-10'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    read_at_month,
    site_id,
    PERCENTILE_APPROX(voltage_bucket, 0.5) AS median_voltage_bucket,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY GROUPING SETS ((quality, read_at_month, site_id), (quality), ())
