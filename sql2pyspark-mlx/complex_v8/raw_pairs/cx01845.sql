WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 10:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_clean,
        s.quality_prefix,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM prepared_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff = 'TOU'
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_prefix,
        s.site_installed_kw,
        si.installed_kw AS site_installed_kw_2,
        si.tariff AS site_tariff
    FROM joined s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, quality, site_installed_kw_2
    FROM with_parent_site
) src
PIVOT (
    SUM(site_installed_kw_2)
    FOR quality IN ('EST', 'BAD', 'OK')
)
