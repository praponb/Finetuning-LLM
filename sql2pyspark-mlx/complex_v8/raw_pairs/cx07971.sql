WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage < 240.1
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_clean,
        s.quality_prefix,
        s.quality_slug,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM base s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw <= 386.6
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.quality_clean,
        s.quality_prefix,
        s.site_name,
        s.site_commissioned_on,
        si.commissioned_on AS site_commissioned_on_2
    FROM with_details_site s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on IS NOT NULL
)
SELECT *
FROM (
    SELECT site_commissioned_on, quality, kwh
    FROM joined
) src
PIVOT (
    SUM(kwh) AS total, MAX(kwh) AS max_v
    FOR quality IN ('BAD', 'EST', 'OK')
)
ORDER BY site_commissioned_on
