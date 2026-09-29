WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter
    FROM meter_readings
    WHERE kwh > 35.9
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
              AND m.voltage >= 223.3
        )
),
quality_stats AS (
    -- Median read_at_quarter per quality
    SELECT quality, PERCENTILE_APPROX(read_at_quarter, 0.5) AS median_read_at_quarter
    FROM without_matches
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_read_at_quarter,
        ROUND(s.read_at_quarter - g.median_read_at_quarter, 2) AS read_at_quarter_minus_median
    FROM without_matches s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.read_at_quarter >= g.median_read_at_quarter
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-07'
              AND si.installed_kw < 284.0
        )
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    read_at_quarter,
    median_read_at_quarter,
    read_at_quarter_minus_median,
    ABS(voltage - 223.3) AS voltage_distance
FROM filtered
WHERE read_at_quarter <= 3
ORDER BY quality
