WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE kwh < 109.3
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_prefix,
        s.kwh_per_voltage,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
combined_details_site AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone
    FROM with_details_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    site_zone,
    site_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null,
    COUNT(DISTINCT site_zone) AS distinct_site_zones,
    ROUND(STDDEV(kwh_per_voltage), 2) AS stddev_kwh_per_voltage
FROM combined_details_site
GROUP BY GROUPING SETS ((site_zone, site_id), (site_zone), ())
ORDER BY site_zone NULLS LAST, site_id NULLS LAST
