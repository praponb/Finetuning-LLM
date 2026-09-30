WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 10:00:00'
      AND site_id >= 2
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        si.installed_kw AS site_installed_kw
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw BETWEEN 96.9 AND 386.6
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        quality,
        site_installed_kw,
        MAX(site_installed_kw) OVER (PARTITION BY quality) AS quality_max_site_installed_kw,
        NTILE(4) OVER (PARTITION BY quality ORDER BY read_at ASC, id) AS read_at_quartile,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY site_installed_kw) AS site_installed_kw_dense_rank,
        site_installed_kw - LAG(site_installed_kw) OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at ASC, id) AS site_installed_kw_change,
        ROUND(AVG(site_installed_kw) OVER (PARTITION BY quality ORDER BY read_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS site_installed_kw_avg_2,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_quality_site_id, 0), 4) AS site_installed_kw_share_of_quality_site_id,
        COUNT(*) OVER w_quality AS rows_in_quality,
        SUM(site_installed_kw) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_installed_kw,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at ASC) AS read_at_dense_rank
    FROM combined_details_site
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_quality AS (PARTITION BY quality)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        quality_max_site_installed_kw,
        read_at_quartile,
        rows_in_quality_site_id,
        site_installed_kw_change,
        site_installed_kw_avg_2,
        rows_in_quality,
        running_site_installed_kw,
        read_at_dense_rank,
        SUM(site_installed_kw_dense_rank) OVER (PARTITION BY quality, read_at_dense_rank ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_installed_kw_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY quality, read_at_dense_rank ORDER BY site_installed_kw ASC, id) AS site_installed_kw_seq,
        LAG(site_installed_kw_avg_2) OVER (PARTITION BY read_at_quartile, quality ORDER BY site_installed_kw_share_of_quality_site_id NULLS LAST, id) AS prev_site_installed_kw_avg_2,
        MAX(read_at_dense_rank) OVER (PARTITION BY quality, read_at_dense_rank ORDER BY site_installed_kw ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS read_at_dense_rank_max_3_prior,
        ROUND(running_site_installed_kw - AVG(running_site_installed_kw) OVER w_read_at_quartile_quality, 2) AS running_site_installed_kw_vs_read_at_quartile_qu
    FROM windowed
    WINDOW w_read_at_quartile_quality AS (PARTITION BY read_at_quartile, quality)
)
-- Final projection
SELECT
    id,
    read_at,
    quality_max_site_installed_kw,
    read_at_quartile,
    rows_in_quality_site_id,
    site_installed_kw_change,
    site_installed_kw_avg_2,
    rows_in_quality,
    running_site_installed_kw,
    read_at_dense_rank,
    running_site_installed_kw_dense_rank,
    site_installed_kw_seq,
    prev_site_installed_kw_avg_2
FROM metrics
ORDER BY read_at_quartile DESC, read_at DESC
