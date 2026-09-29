WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'BAD'
      AND voltage >= 216.1
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
)
-- Subtotals with CUBE
SELECT
    site_id,
    quality,
    ROUND(100 * COUNT(CASE WHEN quality = 'OK' THEN 1 END) / COUNT(*), 1) AS pct_quality_ok,
    ROUND(AVG(kwh), 2) AS avg_kwh,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM without_matches
GROUP BY CUBE(site_id, quality)
