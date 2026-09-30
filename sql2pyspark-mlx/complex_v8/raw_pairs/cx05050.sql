WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.5, 2) AS kwh_adj
    FROM meter_readings
    WHERE quality = 'EST'
      AND site_id >= 5
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_adj,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY quality)) AS distinct_site_zone_in_quality,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        MIN(kwh_adj) OVER (PARTITION BY quality) AS quality_min_kwh_adj,
        DATEDIFF(site_commissioned_on, LAG(site_commissioned_on) OVER (PARTITION BY site_zone ORDER BY voltage DESC, id)) AS days_since_prev_site_commissioned_on,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, site_zone)) AS distinct_site_id_in_quality_site_zone,
        LAG(site_installed_kw, 3) OVER w_all_by_kwh_adj AS prev3_site_installed_kw,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_quality_site_zone, 0), 4) AS site_installed_kw_share_of_quality_site_zone,
        LEAD(read_at, 3) OVER (PARTITION BY site_zone ORDER BY site_commissioned_on ASC NULLS LAST, id) AS next3_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY kwh_adj DESC), 3) AS kwh_adj_cume_dist,
        DENSE_RANK() OVER (ORDER BY site_installed_kw, id) AS site_installed_kw_dense_rank,
        LAG(site_commissioned_on) OVER w_site_id_by_kwh_adj AS prev_site_commissioned_on,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        LAST_VALUE(site_installed_kw) OVER (ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw
    FROM with_parent_site
    WINDOW w_all_by_kwh_adj AS (ORDER BY kwh_adj DESC, id), w_quality_site_zone AS (PARTITION BY quality, site_zone), w_site_id_by_kwh_adj AS (PARTITION BY site_id ORDER BY kwh_adj DESC, id)
)
SELECT
    id,
    site_id,
    read_at,
    site_commissioned_on,
    site_zone,
    site_installed_kw,
    distinct_site_zone_in_quality,
    days_since_prev_site_commissioned_on,
    distinct_site_id_in_quality_site_zone,
    prev3_site_installed_kw,
    next3_read_at,
    kwh_adj_cume_dist,
    site_installed_kw_dense_rank,
    prev_site_commissioned_on,
    distinct_site_id_in_quality,
    last_site_installed_kw
FROM sequenced
ORDER BY id, next3_read_at DESC
