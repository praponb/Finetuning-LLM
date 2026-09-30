WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE voltage > 213.7
      AND kwh > 15.9
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND TO_DATE(m.read_at) < DATE '2025-02-12'
              AND m.quality NOT IN ('EST', 'OK')
        )
)
SELECT
    site_id,
    quality,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM with_activity
GROUP BY GROUPING SETS ((site_id, quality), (site_id), (quality))
