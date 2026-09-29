WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh < 109.0
),
site_id_stats AS (
    -- Median kwh per site_id
    SELECT site_id, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM cleaned_readings
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM cleaned_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh >= g.median_kwh
),
stats AS (
    -- Group-level totals
    SELECT
        CASE WHEN kwh >= 82.9 THEN 'HIGH' WHEN kwh >= 48.2 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        COUNT(*) AS row_count,
        MAX(voltage) AS max_voltage
    FROM outliers
    WHERE voltage >= 216.5
    GROUP BY CASE WHEN kwh >= 82.9 THEN 'HIGH' WHEN kwh >= 48.2 THEN 'MEDIUM' ELSE 'LOW' END
    HAVING COUNT(*) >= 2
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE max_voltage > (SELECT AVG(max_voltage) FROM stats)
)
-- Final projection
SELECT
    kwh_tier,
    max_voltage,
    LEAST(max_voltage, row_count) AS least_max_voltage_row_count,
    COALESCE(kwh_tier, 'UNKNOWN') AS kwh_tier_or_unknown
FROM screened
ORDER BY kwh_tier_or_unknown, max_voltage, kwh_tier
LIMIT 10
