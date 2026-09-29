WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE quality IS NOT NULL
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM scoped s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.quality_clean,
        s.last_reading_read_at,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM with_counts s
    LEFT JOIN sites si ON si.id = s.site_id
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.site_installed_kw,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on_2,
        si.tariff AS site_tariff
    FROM combined_details s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'TOU'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_zone, read_at, quality, site_installed_kw
    FROM joined_site
) src
PIVOT (
    MIN(site_installed_kw)
    FOR quality IN ('EST', 'BAD')
)
ORDER BY site_zone, read_at
