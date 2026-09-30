WITH recent AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND quality NOT IN ('EST', 'BAD')
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        site_installed_kw,
        ROUND(PERCENT_RANK() OVER w_all_by_site_commissioned_on, 3) AS site_commissioned_on_percent_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY quality), 2) AS quality_avg_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(read_at) ORDER BY site_commissioned_on), 3) AS site_commissioned_on_percent_rank_2,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        DENSE_RANK() OVER (ORDER BY site_commissioned_on) AS site_commissioned_on_dense_rank,
        ROUND(CUME_DIST() OVER w_quality_by_kwh, 3) AS kwh_cume_dist,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY quality), 0), 4) AS site_installed_kw_share_of_quality,
        ROUND(AVG(site_installed_kw) OVER (ORDER BY site_commissioned_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS site_installed_kw_avg_2_prior
    FROM joined_site
    WINDOW w_all_by_site_commissioned_on AS (ORDER BY site_commissioned_on, id), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    site_installed_kw,
    site_commissioned_on_percent_rank,
    quality_avg_kwh,
    site_commissioned_on_percent_rank_2,
    distinct_site_id_in_quality,
    site_commissioned_on_dense_rank,
    site_installed_kw_share_of_quality,
    site_installed_kw_avg_2_prior,
    ROUND(kwh_cume_dist * 1.1, 2) AS kwh_cume_dist_adj,
    kwh_cume_dist - site_installed_kw AS kwh_cume_dist_minus_site_installed_kw
FROM with_trends
WHERE site_installed_kw_avg_2_prior <= 277.2
ORDER BY kwh_cume_dist_adj, kwh
