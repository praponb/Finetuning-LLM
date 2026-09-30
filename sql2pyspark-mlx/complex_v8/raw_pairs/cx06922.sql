WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.quality,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND HOUR(m.read_at) BETWEEN 7 AND 13
        )
)
SELECT
    id,
    site_id,
    quality,
    site_commissioned_on
FROM with_activity
WHERE site_installed_kw <= 373.3
ORDER BY site_id DESC, id
LIMIT 5
