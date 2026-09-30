WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 6 AND 10 OR site_id IS NOT NULL)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_readings
    WHERE kwh > (SELECT AVG(kwh) FROM prepared_readings)
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-01-19 13:00:00'
        )
      AND s.kwh BETWEEN 30.2 AND 110.5
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM with_activity s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on >= DATE '2024-09-15'
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    site_commissioned_on,
    site_zone,
    CONCAT_WS(' / ', site_zone, quality) AS site_zone_quality_label,
    DATEDIFF(DATE '2025-06-30', site_commissioned_on) AS days_since_site_commissioned_on
FROM with_parent
ORDER BY kwh DESC, site_commissioned_on
