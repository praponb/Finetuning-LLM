WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        site_installed_kw,
        site_name,
        MAX(site_installed_kw) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY site_installed_kw, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS site_installed_kw_max_7,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_read_at_month, 0), 4) AS kwh_share_of_read_at_month,
        SUM(site_installed_kw) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_site_installed_kw,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        ROUND(AVG(kwh) OVER w_site_id, 2) AS site_id_avg_kwh,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY kwh, id), 3) AS kwh_cume_dist,
        MIN(site_installed_kw) OVER (PARTITION BY site_id) AS site_id_min_site_installed_kw,
        LAG(site_installed_kw, 3) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh, id) AS prev3_site_installed_kw,
        FIRST_VALUE(site_installed_kw) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_installed_kw,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at)), 2) AS read_at_month_avg_kwh
    FROM combined_details_site
    WINDOW w_read_at_month AS (PARTITION BY MONTH(read_at)), w_site_id AS (PARTITION BY site_id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        site_installed_kw,
        site_name,
        site_installed_kw_max_7,
        kwh_share_of_read_at_month,
        running_site_installed_kw,
        minutes_since_prev_read_at,
        site_id_min_kwh,
        site_id_avg_kwh,
        second_read_at,
        kwh_cume_dist,
        first_known_site_installed_kw,
        CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref,
        FLOOR(site_installed_kw / 100) * 100 AS site_installed_kw_bucket
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_installed_kw,
        site_name,
        kwh_share_of_read_at_month,
        running_site_installed_kw,
        minutes_since_prev_read_at,
        site_id_min_kwh,
        site_id_avg_kwh,
        second_read_at,
        kwh_cume_dist,
        ROUND(AVG(site_id_min_kwh) OVER (ORDER BY site_installed_kw, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS site_id_min_kwh_avg_7,
        ROUND(kwh_cume_dist - AVG(kwh_cume_dist) OVER (PARTITION BY MONTH(second_read_at)), 2) AS kwh_cume_dist_vs_second_read_at_month_avg,
        ROUND(minutes_since_prev_read_at / NULLIF(SUM(minutes_since_prev_read_at) OVER (PARTITION BY MONTH(second_read_at)), 0), 4) AS minutes_since_prev_read_at_share_of_second_read_,
        ROUND(CUME_DIST() OVER (ORDER BY site_installed_kw_bucket DESC, id), 3) AS site_installed_kw_bucket_cume_dist,
        LAG(site_id_avg_kwh, 1, 0) OVER w_all_by_kwh_cume_dist AS prev_site_id_avg_kwh
    FROM annotated
    WINDOW w_all_by_kwh_cume_dist AS (ORDER BY kwh_cume_dist, id)
)
SELECT
    id,
    site_installed_kw,
    kwh_share_of_read_at_month,
    running_site_installed_kw,
    minutes_since_prev_read_at,
    site_id_min_kwh,
    second_read_at,
    kwh_cume_dist,
    site_id_min_kwh_avg_7,
    kwh_cume_dist_vs_second_read_at_month_avg,
    site_installed_kw_bucket_cume_dist,
    SUBSTRING(TRIM(site_name), 1, 2) AS site_name_prefix,
    LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
FROM metrics
ORDER BY kwh_share_of_read_at_month DESC, site_installed_kw DESC
