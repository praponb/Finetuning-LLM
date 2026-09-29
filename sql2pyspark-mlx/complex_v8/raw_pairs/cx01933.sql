WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.1, 2) AS kwh_adj
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 07:00:00'
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.kwh_adj,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.site_tariff,
        s.site_zone,
        si.zone AS site_zone_2,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff_2
    FROM enriched s
    JOIN sites si ON si.id = s.site_id AND si.zone = 'Z3'
)
-- Subtotals with CUBE
SELECT
    site_id,
    site_tariff_2,
    SUM(kwh) FILTER (WHERE site_zone <> 'Z4') AS kwh_site_zone_z4
FROM enriched_site
GROUP BY CUBE(site_id, site_tariff_2)
