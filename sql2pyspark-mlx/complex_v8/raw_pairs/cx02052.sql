WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 86.0
      AND MONTH(read_at) IN (1, 2)
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        si.tariff AS site_tariff,
        si.site_name
    FROM base_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, site_tariff), 2) AS quality_site_tariff_avg_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_tariff), 0), 4) AS kwh_share_of_quality_site_tariff,
        NTILE(3) OVER w_site_tariff_quality_by_kwh AS kwh_tercile,
        LAST_VALUE(kwh) OVER (PARTITION BY site_tariff, quality ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        NTILE(10) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC NULLS FIRST, id) AS kwh_decile,
        LAG(kwh) OVER w_site_tariff_quality_by_kwh AS prev_kwh,
        SUM(kwh) OVER (PARTITION BY quality, site_tariff) AS quality_site_tariff_sum_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY site_tariff, quality ORDER BY kwh), 3) AS kwh_cume_dist,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        COUNT(*) OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(site_id) OVER w_quality_site_tariff) AS distinct_site_id_in_quality_site_tariff,
        NTILE(10) OVER (PARTITION BY site_tariff, quality ORDER BY kwh, id) AS kwh_decile_2,
        MIN(kwh) OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far
    FROM combined_details_site
    WINDOW w_site_tariff_quality_by_kwh AS (PARTITION BY site_tariff, quality ORDER BY kwh, id), w_quality_site_tariff AS (PARTITION BY quality, site_tariff)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        quality,
        site_tariff,
        site_name,
        quality_site_tariff_avg_kwh,
        kwh_share_of_quality_site_tariff,
        kwh_tercile,
        last_kwh,
        kwh_decile,
        prev_kwh,
        kwh_cume_dist,
        kwh_avg_2_prior,
        running_count,
        distinct_site_id_in_quality_site_tariff,
        kwh_decile_2,
        min_kwh_so_far,
        NTILE(4) OVER (PARTITION BY kwh_tercile, distinct_site_id_in_quality_site_tariff ORDER BY kwh ASC, id) AS kwh_quartile,
        COUNT(*) OVER (PARTITION BY kwh_decile_2) AS rows_in_kwh_decile_2,
        FIRST_VALUE(running_count) OVER (PARTITION BY distinct_site_id_in_quality_site_tariff, site_id ORDER BY kwh_decile_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_running_count
    FROM with_windows
),
grouped AS (
    -- Summarise per group
    SELECT
        kwh_quartile,
        kwh_decile_2,
        quality,
        ROUND(SUM(last_kwh) / NULLIF(SUM(kwh_share_of_quality_site_tariff), 0), 3) AS last_kwh_to_kwh_share_of_quality_site_tariff_rat,
        ROUND(SUM(prev_kwh) / NULLIF(SUM(distinct_site_id_in_quality_site_tariff), 0), 3) AS prev_kwh_to_distinct_site_id_in_quality_site_tar,
        COUNT(DISTINCT site_tariff) AS distinct_site_tariffs,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        MAX(prev_kwh) AS max_prev_kwh
    FROM metrics
    GROUP BY kwh_quartile, kwh_decile_2, quality
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY kwh_quartile ORDER BY last_kwh_to_kwh_share_of_quality_site_tariff_rat) AS last_kwh_to_kwh_share_of_quality_site_tariff_rat_2,
        distinct_site_tariffs - LAG(distinct_site_tariffs) OVER (PARTITION BY distinct_site_tariffs ORDER BY last_kwh_to_kwh_share_of_quality_site_tariff_rat DESC NULLS FIRST, kwh_quartile, kwh_decile_2, quality) AS distinct_site_tariffs_change,
        LEAD(distinct_site_tariffs, 3) OVER w_kwh_quartile_by_last_kwh_to_2 AS next3_distinct_site_tariffs,
        RANK() OVER (PARTITION BY distinct_site_tariffs ORDER BY last_kwh_to_kwh_share_of_quality_site_tariff_rat DESC NULLS FIRST, kwh_quartile, kwh_decile_2, quality) AS last_kwh_to_kwh_share_of_quality_site_tariff_rat_3
    FROM grouped
    WINDOW w_kwh_quartile_by_last_kwh_to_2 AS (PARTITION BY kwh_quartile ORDER BY last_kwh_to_kwh_share_of_quality_site_tariff_rat, kwh_quartile, kwh_decile_2, quality)
)
-- Final projection
SELECT
    kwh_quartile,
    distinct_site_tariffs,
    site_name_list,
    max_prev_kwh,
    last_kwh_to_kwh_share_of_quality_site_tariff_rat_2,
    distinct_site_tariffs_change,
    next3_distinct_site_tariffs,
    ABS(kwh_quartile - 6) AS kwh_quartile_distance,
    LOWER(TRIM(quality)) AS quality_clean
FROM with_trends
WHERE prev_kwh_to_distinct_site_id_in_quality_site_tar >= 4.0
ORDER BY distinct_site_tariffs_change, distinct_site_tariffs
