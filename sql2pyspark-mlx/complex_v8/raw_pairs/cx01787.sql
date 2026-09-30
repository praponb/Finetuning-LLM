WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        ROUND(voltage * 0.9, 2) AS voltage_adj
    FROM meter_readings
    WHERE quality = 'OK'
      AND site_id <= 8
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_clean,
        s.days_since_read_at,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.days_since_read_at,
        s.site_zone,
        s.site_installed_kw,
        si.zone AS site_zone_2,
        si.installed_kw AS site_installed_kw_2
    FROM enriched s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z4'
)
SELECT
    quality,
    site_zone,
    site_id,
    GROUPING_ID() AS gid,
    ROUND(AVG(days_since_read_at), 2) AS avg_days_since_read_at,
    ARRAY_SORT(COLLECT_SET(site_zone)) AS site_zone_list,
    COUNT(*) FILTER (WHERE site_zone_2 NOT IN ('Z3', 'Z4')) AS n_site_zone_2_not_in_z3_z4
FROM joined
GROUP BY ROLLUP(quality, site_zone, site_id)
ORDER BY quality NULLS LAST, site_zone NULLS LAST, site_id NULLS LAST
