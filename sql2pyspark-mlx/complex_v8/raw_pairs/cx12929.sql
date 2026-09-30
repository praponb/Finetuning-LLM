WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE (site_id <= 9 OR voltage > 222.6)
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_slug,
        si.installed_kw AS site_installed_kw
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        quality,
        site_installed_kw,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, quality ORDER BY site_installed_kw DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        DENSE_RANK() OVER (PARTITION BY site_id, quality ORDER BY site_installed_kw DESC) AS site_installed_kw_dense_rank,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, quality ORDER BY site_installed_kw, id), 3) AS site_installed_kw_percent_rank,
        FIRST_VALUE(site_installed_kw) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_installed_kw,
        LAG(kwh, 2) OVER (PARTITION BY site_id ORDER BY kwh DESC, id) AS prev2_kwh,
        FIRST_VALUE(site_installed_kw) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_installed_kw_2,
        SUM(site_installed_kw) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS site_installed_kw_sum_3_prior,
        MAX(kwh) OVER (PARTITION BY site_id, quality ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        ROUND(AVG(site_installed_kw) OVER (ORDER BY site_installed_kw, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS site_installed_kw_avg_6_prior,
        LAST_VALUE(voltage) OVER (PARTITION BY quality, site_id ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER w_site_id_quality, 2) AS site_installed_kw_vs_site_id_quality_avg,
        NTILE(4) OVER w_site_id_quality_by_site_inst_3 AS site_installed_kw_quartile
    FROM enriched_site
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality), w_site_id AS (PARTITION BY site_id), w_site_id_quality_by_site_inst_3 AS (PARTITION BY site_id, quality ORDER BY site_installed_kw, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        site_id,
        kwh,
        quality,
        site_installed_kw,
        voltage_avg_3_prior,
        rows_in_site_id_quality,
        kwh_vs_site_id_avg,
        distinct_quality_in_site_id,
        site_installed_kw_percent_rank,
        first_known_site_installed_kw,
        prev2_kwh,
        first_known_site_installed_kw_2,
        site_installed_kw_sum_3_prior,
        max_kwh_so_far,
        site_installed_kw_avg_6_prior,
        last_voltage,
        site_installed_kw_vs_site_id_quality_avg,
        site_installed_kw_quartile
    FROM with_trends
    WHERE site_installed_kw_dense_rank = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        distinct_quality_in_site_id - LAG(distinct_quality_in_site_id) OVER w_quality_by_first_known_site AS distinct_quality_in_site_id_change,
        ROUND(rows_in_site_id_quality - AVG(rows_in_site_id_quality) OVER (PARTITION BY site_installed_kw_quartile), 2) AS rows_in_site_id_quality_vs_site_installed_kw_qua,
        FIRST_VALUE(kwh_vs_site_id_avg) IGNORE NULLS OVER (PARTITION BY quality ORDER BY first_known_site_installed_kw_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh_vs_site_id_avg,
        ROUND(AVG(distinct_quality_in_site_id) OVER (PARTITION BY site_installed_kw_quartile), 2) AS site_installed_kw_quartile_avg_distinct_quality_,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY distinct_quality_in_site_id DESC NULLS FIRST, id) AS seq_no,
        NTH_VALUE(kwh_vs_site_id_avg, 2) OVER (PARTITION BY quality ORDER BY distinct_quality_in_site_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_vs_site_id_avg
    FROM leaders
    WINDOW w_quality_by_first_known_site AS (PARTITION BY quality ORDER BY first_known_site_installed_kw_2 DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        distinct_quality_in_site_id,
        site_id,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(100 * COUNT(CASE WHEN quality = 'EST' THEN 1 END) / COUNT(*), 1) AS pct_quality_est,
        COUNT(*) FILTER (WHERE quality IN ('BAD', 'OK')) AS n_quality_in_bad_ok,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE quality IN ('EST', 'BAD')) AS n_quality_in_est_bad,
        ROUND(SUM(distinct_quality_in_site_id) / NULLIF(SUM(site_installed_kw), 0), 3) AS distinct_quality_in_site_id_to_site_installed_kw
    FROM windowed
    GROUP BY distinct_quality_in_site_id, site_id
    HAVING MAX(site_installed_kw) > 141.6
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        distinct_quality_in_site_id,
        site_id,
        quality_list,
        n_quality_in_est_bad,
        distinct_quality_in_site_id_to_site_installed_kw,
        ROUND(AVG(pct_quality_est) OVER (PARTITION BY distinct_quality_in_site_id), 2) AS distinct_quality_in_site_id_avg_pct_quality_est,
        NTILE(3) OVER (PARTITION BY n_quality_in_bad_ok ORDER BY n_quality_in_est_bad, distinct_quality_in_site_id, site_id) AS n_quality_in_est_bad_tercile,
        SUM(cnt) OVER (PARTITION BY cnt ORDER BY cnt, distinct_quality_in_site_id, site_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt_sum_3
    FROM grouped
)
-- Final projection
SELECT
    distinct_quality_in_site_id,
    site_id,
    quality_list,
    n_quality_in_est_bad,
    distinct_quality_in_site_id_to_site_installed_kw,
    distinct_quality_in_site_id_avg_pct_quality_est,
    n_quality_in_est_bad_tercile,
    cnt_sum_3,
    ABS(cnt_sum_3 - 24) AS cnt_sum_3_distance,
    ROUND(n_quality_in_est_bad / NULLIF(cnt_sum_3, 0), 3) AS n_quality_in_est_bad_per_cnt_sum_3
FROM with_windows
ORDER BY site_id NULLS LAST
