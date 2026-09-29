WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (TO_DATE(read_at) < DATE '2025-02-12' OR kwh BETWEEN 35.0 AND 106.5)
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.quality,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        site_name,
        site_installed_kw,
        SUM(site_installed_kw) OVER (PARTITION BY site_zone, quality) AS site_zone_quality_sum_site_installed_kw,
        NTILE(3) OVER (PARTITION BY site_zone ORDER BY site_installed_kw ASC, id) AS site_installed_kw_tercile,
        LAST_VALUE(site_installed_kw) IGNORE NULLS OVER (PARTITION BY quality ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_installed_kw,
        COUNT(*) OVER (PARTITION BY quality, site_zone) AS rows_in_quality_site_zone,
        MAX(site_installed_kw) OVER (PARTITION BY site_zone, quality) AS site_zone_quality_max_site_installed_kw,
        DENSE_RANK() OVER w_site_id_quality_by_site_inst AS site_installed_kw_dense_rank,
        LAST_VALUE(site_installed_kw) OVER (ORDER BY site_installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_zone ORDER BY site_installed_kw ASC, id), 3) AS site_installed_kw_percent_rank,
        ROUND(PERCENT_RANK() OVER w_quality_site_zone_by_site_in, 3) AS site_installed_kw_percent_rank_2,
        NTILE(4) OVER w_site_zone_by_site_installed AS site_installed_kw_quartile,
        SUM(site_installed_kw) OVER (PARTITION BY site_zone ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_installed_kw,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_zone, quality)) AS distinct_site_id_in_site_zone_quality
    FROM joined_site
    WINDOW w_site_id_quality_by_site_inst AS (PARTITION BY site_id, quality ORDER BY site_installed_kw DESC, id), w_quality_site_zone_by_site_in AS (PARTITION BY quality, site_zone ORDER BY site_installed_kw, id), w_site_zone_by_site_installed AS (PARTITION BY site_zone ORDER BY site_installed_kw ASC, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        site_id,
        site_name,
        site_installed_kw,
        site_zone_quality_sum_site_installed_kw,
        site_installed_kw_tercile,
        last_known_site_installed_kw,
        rows_in_quality_site_zone,
        site_zone_quality_max_site_installed_kw,
        last_site_installed_kw,
        site_installed_kw_percent_rank,
        site_installed_kw_percent_rank_2,
        site_installed_kw_quartile,
        running_site_installed_kw,
        distinct_site_id_in_site_zone_quality
    FROM ranked
    WHERE site_installed_kw_dense_rank = 1
      AND site_installed_kw_percent_rank_2 < 0.9
)
SELECT
    id,
    site_name,
    site_zone_quality_sum_site_installed_kw,
    site_installed_kw_tercile,
    last_known_site_installed_kw,
    last_site_installed_kw,
    site_installed_kw_percent_rank,
    running_site_installed_kw,
    distinct_site_id_in_site_zone_quality
FROM top_per_group
ORDER BY last_known_site_installed_kw, site_zone_quality_sum_site_installed_kw NULLS LAST, id
LIMIT 5
