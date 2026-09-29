WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        COALESCE(kwh, 0.0) AS kwh_filled_2
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.kwh_filled,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM base_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_quality_site_id, 0), 4) AS site_installed_kw_share_of_quality_site_id,
        SUM(kwh) OVER (ORDER BY read_at NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_sum_6_prior,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY quality, site_id ORDER BY kwh_filled DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        FIRST_VALUE(site_commissioned_on) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality ORDER BY kwh_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_commissioned_on,
        ROUND(kwh_filled - AVG(kwh_filled) OVER w_site_id_quality, 2) AS kwh_filled_vs_site_id_quality_avg,
        DENSE_RANK() OVER w_quality_by_read_at_ties AS read_at_dense_rank,
        MIN(kwh_filled) OVER w_quality_site_id AS quality_site_id_min_kwh_filled,
        COUNT(*) OVER (PARTITION BY site_id, quality) AS rows_in_site_id_quality,
        MIN(kwh) OVER (PARTITION BY site_id, quality) AS site_id_quality_min_kwh,
        ROUND(AVG(site_installed_kw) OVER (PARTITION BY quality, site_id), 2) AS quality_site_id_avg_site_installed_kw
    FROM joined
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_site_id_quality AS (PARTITION BY site_id, quality), w_quality_by_read_at_ties AS (PARTITION BY quality ORDER BY read_at)
)
SELECT
    id,
    read_at,
    kwh,
    quality,
    kwh_filled,
    site_installed_kw,
    site_name,
    site_commissioned_on,
    kwh_sum_6_prior,
    minutes_since_prev_read_at,
    kwh_filled_vs_site_id_quality_avg,
    read_at_dense_rank,
    quality_site_id_min_kwh_filled,
    rows_in_site_id_quality,
    site_id_quality_min_kwh,
    quality_site_id_avg_site_installed_kw,
    rows_in_site_id_quality + site_installed_kw AS rows_in_site_id_quality_plus_site_installed_kw
FROM windowed
WHERE quality_site_id_min_kwh_filled BETWEEN 180.0 AND 1020.0
ORDER BY site_id_quality_min_kwh DESC, kwh_filled_vs_site_id_quality_avg NULLS LAST
