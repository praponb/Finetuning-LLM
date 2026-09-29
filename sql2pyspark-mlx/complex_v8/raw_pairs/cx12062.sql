WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM scoped_readings
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_prefix,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM tagged s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on < DATE '2024-11-21'
)
SELECT *
FROM (
    SELECT site_commissioned_on, read_at, quality, site_installed_kw
    FROM with_parent
) src
PIVOT (
    MIN(site_installed_kw)
    FOR quality IN ('OK', 'EST', 'BAD')
)
