WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN voltage >= 232.4 THEN 'A' WHEN voltage >= 220.7 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
    WHERE (quality IN ('BAD', 'EST') OR site_id IS NOT NULL)
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        si.installed_kw AS site_installed_kw
    FROM cleaned s
    LEFT JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        quality,
        site_installed_kw,
        NTILE(4) OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id) AS read_at_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id), 3) AS read_at_cume_dist,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_quality_site_id, 0), 4) AS site_installed_kw_share_of_quality_site_id,
        ROUND(AVG(site_installed_kw) OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS site_installed_kw_avg_2,
        MAX(site_installed_kw) OVER (PARTITION BY site_id ORDER BY site_installed_kw ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS site_installed_kw_max_1_prior,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_installed_kw ASC NULLS LAST, id) AS site_installed_kw_seq,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY read_at ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        MIN(site_installed_kw) OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_site_installed_kw_so_far
    FROM with_parent_site
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        read_at,
        quality,
        site_installed_kw,
        read_at_quartile,
        read_at_cume_dist,
        rows_in_quality,
        site_installed_kw_share_of_quality_site_id,
        site_installed_kw_avg_2,
        site_installed_kw_max_1_prior,
        distinct_site_id_in_quality,
        minutes_since_prev_read_at,
        min_site_installed_kw_so_far
    FROM metrics
    WHERE site_installed_kw_seq <= 3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(read_at_quartile) OVER (PARTITION BY distinct_site_id_in_quality, read_at_quartile ORDER BY distinct_site_id_in_quality DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS read_at_quartile_avg_7,
        ROW_NUMBER() OVER w_all_by_distinct_site_id_in_q AS seq_no,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at), 3) AS read_at_percent_rank,
        LAG(site_installed_kw_avg_2) OVER w_all_by_read_at AS prev_site_installed_kw_avg_2,
        LAG(read_at_cume_dist, 2) OVER w_distinct_site_id_in_quality_2 AS prev2_read_at_cume_dist,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality_2,
        LAG(rows_in_quality) OVER (ORDER BY read_at_cume_dist, id) AS prev_rows_in_quality,
        distinct_site_id_in_quality - LAG(distinct_site_id_in_quality) OVER (ORDER BY read_at_cume_dist, id) AS distinct_site_id_in_quality_change
    FROM leaders
    WINDOW w_all_by_distinct_site_id_in_q AS (ORDER BY distinct_site_id_in_quality, id), w_all_by_read_at AS (ORDER BY read_at, id), w_distinct_site_id_in_quality_2 AS (PARTITION BY distinct_site_id_in_quality, read_at_quartile ORDER BY distinct_site_id_in_quality DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    site_installed_kw,
    read_at_quartile,
    read_at_cume_dist,
    rows_in_quality,
    site_installed_kw_share_of_quality_site_id,
    site_installed_kw_avg_2,
    minutes_since_prev_read_at,
    min_site_installed_kw_so_far,
    read_at_quartile_avg_7,
    prev2_read_at_cume_dist,
    rows_in_quality_2,
    prev_rows_in_quality,
    distinct_site_id_in_quality_change,
    INITCAP(TRIM(quality)) AS quality_clean,
    CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
FROM windowed
ORDER BY site_id NULLS LAST, min_site_installed_kw_so_far DESC, id
LIMIT 5
