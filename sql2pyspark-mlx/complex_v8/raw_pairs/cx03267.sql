WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        ROUND(kwh * 0.25, 2) AS kwh_adj
    FROM meter_readings
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh BETWEEN 15.9 AND 117.4
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(kwh) AS min_reading_kwh,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_kwh,
        m.first_reading_read_at
    FROM filtered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN min_reading_kwh >= 72.0 THEN 'A' WHEN min_reading_kwh >= 24.0 THEN 'B' ELSE 'C' END AS min_reading_kwh_tier
    FROM with_stats
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(min_reading_kwh_tier, 'ALL') AS min_reading_kwh_tier,
    site_id,
    ROUND(STDDEV(kwh_adj), 2) AS stddev_kwh_adj,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    ROUND(STDDEV(voltage), 2) AS stddev_voltage
FROM tagged
GROUP BY GROUPING SETS ((quality, min_reading_kwh_tier, site_id), (quality), ())
ORDER BY quality NULLS LAST, min_reading_kwh_tier NULLS LAST, site_id NULLS LAST
