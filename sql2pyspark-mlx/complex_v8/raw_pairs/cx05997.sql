WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE voltage > 222.6
      AND TO_DATE(read_at) < DATE '2025-02-06'
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_len,
        si.zone AS site_zone
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        quality_len,
        site_zone,
        ROUND(CUME_DIST() OVER w_site_zone_read_at_month_by_q, 3) AS quality_len_cume_dist,
        MIN(quality_len) OVER (PARTITION BY quality_len) AS quality_len_min_quality_len,
        ROUND(PERCENT_RANK() OVER w_quality_len_by_kwh_ties, 3) AS kwh_percent_rank,
        SUM(quality_len) OVER (PARTITION BY site_zone) AS site_zone_sum_quality_len,
        SUM(kwh) OVER w_quality_len AS quality_len_sum_kwh,
        RANK() OVER (PARTITION BY site_id, quality_len ORDER BY kwh DESC, id) AS kwh_rank,
        DENSE_RANK() OVER (PARTITION BY site_id, quality_len ORDER BY kwh DESC) AS kwh_dense_rank,
        NTILE(4) OVER w_site_id_quality_len_by_kwh AS kwh_quartile,
        ROUND(AVG(kwh) OVER (PARTITION BY site_zone), 2) AS site_zone_avg_kwh,
        SUM(quality_len) OVER (PARTITION BY site_zone ORDER BY quality_len, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quality_len_sum_4
    FROM with_details_site
    WINDOW w_site_zone_read_at_month_by_q AS (PARTITION BY site_zone, DATE_TRUNC('MONTH', read_at) ORDER BY quality_len NULLS LAST), w_quality_len_by_kwh_ties AS (PARTITION BY quality_len ORDER BY kwh DESC), w_quality_len AS (PARTITION BY quality_len), w_site_id_quality_len_by_kwh AS (PARTITION BY site_id, quality_len ORDER BY kwh DESC, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        quality_len,
        quality_len_cume_dist,
        quality_len_min_quality_len,
        kwh_percent_rank,
        site_zone_sum_quality_len,
        quality_len_sum_kwh,
        kwh_rank,
        site_zone_avg_kwh,
        quality_len_sum_4,
        MIN(kwh_quartile) OVER (ORDER BY quality_len_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_quartile_so_far,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY quality_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        LAST_VALUE(kwh) OVER (PARTITION BY quality_len ORDER BY kwh_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        SIZE(COLLECT_SET(site_id) OVER w_site_zone_kwh_quartile) AS distinct_site_id_in_site_zone_kwh_quartile,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY kwh_dense_rank)) AS distinct_site_id_in_kwh_dense_rank
    FROM windowed
    WINDOW w_site_zone_kwh_quartile AS (PARTITION BY site_zone, kwh_quartile)
)
-- Final projection
SELECT
    id,
    site_id,
    quality_len,
    quality_len_min_quality_len,
    site_zone_sum_quality_len,
    quality_len_sum_kwh,
    site_zone_avg_kwh,
    min_kwh_quartile_so_far,
    first_known_kwh,
    last_kwh,
    distinct_site_id_in_site_zone_kwh_quartile,
    distinct_site_id_in_kwh_dense_rank,
    ROUND(quality_len_sum_kwh * 0.25, 2) AS quality_len_sum_kwh_adj
FROM with_trends
WHERE quality_len <= 12
ORDER BY site_id DESC, id
LIMIT 10
