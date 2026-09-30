WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 229.9 THEN 'large' WHEN voltage >= 219.5 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
),
quality_stats AS (
    -- Median kwh per quality
    SELECT quality, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM valid_readings
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM valid_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh >= g.median_kwh
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        kwh - median_kwh AS kwh_minus_median_kwh
    FROM above_average
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, median_kwh, kwh, kwh_minus_median
    FROM tagged
) src
UNPIVOT (
    metric_value FOR field IN (median_kwh AS median_kwh_v, kwh AS m_kwh, kwh_minus_median AS m_kwh_minus_median)
) unp
