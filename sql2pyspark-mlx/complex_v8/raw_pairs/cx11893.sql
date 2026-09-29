WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage < 231.7
      AND read_at < TIMESTAMP '2025-02-12 14:00:00'
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality <> 'EST'
              AND m.read_at < TIMESTAMP '2025-03-02 11:00:00'
        )
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.read_at_year,
        s.quality_slug,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM without_matches s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw > 141.6
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.read_at_year,
        s.quality_slug,
        s.site_installed_kw,
        s.site_commissioned_on,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw_2
    FROM with_details_site s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff IN ('TOU', 'EV')
)
SELECT *
FROM (
    SELECT site_zone, quality, site_installed_kw
    FROM joined_site
) src
PIVOT (
    SUM(site_installed_kw)
    FOR quality IN ('OK', 'BAD', 'EST')
)
