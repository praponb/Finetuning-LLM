WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE quality = 'OK'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on >= DATE '2024-09-15'
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.site_tariff,
        si.tariff AS site_tariff_2,
        si.zone AS site_zone
    FROM joined s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    site_tariff,
    site_tariff_2,
    site_zone
FROM joined_site
WHERE kwh > 45.0
ORDER BY read_at DESC, site_zone DESC NULLS FIRST
