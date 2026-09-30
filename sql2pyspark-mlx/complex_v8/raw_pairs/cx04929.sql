WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh > 33.9
),
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM base_readings s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff NOT IN ('TOU', 'EV')
        )
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    site_installed_kw,
    site_tariff,
    CONCAT_WS(' / ', quality, site_tariff) AS quality_site_tariff_label
FROM with_details
ORDER BY site_installed_kw DESC NULLS FIRST, site_id DESC NULLS FIRST, id
LIMIT 10
