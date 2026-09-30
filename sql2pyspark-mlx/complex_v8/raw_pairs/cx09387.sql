WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 87.1 THEN 'A' WHEN kwh >= 45.3 THEN 'B' ELSE 'C' END AS kwh_tier,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE kwh <= 80.8
      AND voltage >= 218.5
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_tier,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_tier,
        s.site_installed_kw,
        s.site_zone,
        si.installed_kw AS site_installed_kw_2
    FROM joined s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff <> 'FLAT'
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_tier,
        s.site_zone,
        si.installed_kw AS site_installed_kw_3
    FROM with_parent s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(kwh_tier, 'ALL') AS kwh_tier,
    GROUPING(quality) AS is_total_quality,
    MIN(read_at) AS first_read_at,
    ARRAY_SORT(COLLECT_SET(site_zone)) AS site_zone_list,
    SUM(CASE WHEN site_zone = 'Z4' THEN 1 ELSE 0 END) AS n_site_zone_z4
FROM with_details_site
GROUP BY ROLLUP(quality, kwh_tier)
ORDER BY quality NULLS LAST, kwh_tier NULLS LAST
