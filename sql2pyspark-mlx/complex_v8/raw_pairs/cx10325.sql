WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE site_id >= 9
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_readings
    WHERE voltage_per_kwh > (SELECT AVG(voltage_per_kwh) FROM prepared_readings)
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_label,
        s.voltage_per_kwh,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    COALESCE(site_zone, 'ALL') AS site_zone,
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    MAX(read_at) AS last_read_at
FROM with_parent_site
GROUP BY GROUPING SETS ((site_zone, quality, site_id), (site_zone), ())
ORDER BY site_zone NULLS LAST, quality NULLS LAST, site_id NULLS LAST
