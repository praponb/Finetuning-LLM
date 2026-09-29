WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE quality IN ('BAD', 'OK')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.quality_ref,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM cleaned_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone <> 'Z2'
              AND si.tariff IN ('FLAT', 'TOU')
        )
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality_ref,
        s.site_zone,
        s.site_tariff,
        si.installed_kw AS site_installed_kw
    FROM screened s
    JOIN sites si ON si.id = s.site_id
),
with_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone_2,
        si.site_name,
        si.installed_kw AS site_installed_kw_2
    FROM with_parent_site s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    quality_ref,
    site_zone,
    site_tariff,
    site_installed_kw_2
FROM with_details_site
WHERE site_installed_kw > 93.3
ORDER BY site_zone DESC, id
LIMIT 10
