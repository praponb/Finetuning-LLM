WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE quality IN ('BAD', 'EST')
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.commissioned_on AS site_commissioned_on, si.zone AS site_zone
    FROM base s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z4'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        quality,
        quality_len,
        site_commissioned_on,
        LAST_VALUE(read_at) OVER (PARTITION BY MONTH(read_at) ORDER BY quality_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY quality_len)) AS distinct_site_zone_in_quality_len,
        MIN(kwh) OVER (PARTITION BY site_id, quality_len) AS site_id_quality_len_min_kwh,
        LEAD(read_at_date, 3) OVER w_quality_by_quality_len AS next3_read_at_date,
        FIRST_VALUE(quality_len) IGNORE NULLS OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quality_len,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY site_id, quality_len)) AS distinct_site_zone_in_site_id_quality_len,
        FIRST_VALUE(quality_len) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quality_len_2,
        ROUND(CUME_DIST() OVER w_read_at_month_by_quality_len_2, 3) AS quality_len_cume_dist,
        COUNT(*) OVER (PARTITION BY site_id, quality_len) AS rows_in_site_id_quality_len,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY quality_len ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY quality_len ASC, id) AS quality_len_seq,
        DATEDIFF(read_at_date, LAG(read_at_date) OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on ASC, id)) AS days_since_prev_read_at_date,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY MONTH(read_at) ORDER BY quality_len ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(quality_len / NULLIF(SUM(quality_len) OVER w_quality_len, 0), 4) AS quality_len_share_of_quality_len,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality_len), 0), 4) AS kwh_share_of_quality_len,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality_len ORDER BY read_at_date DESC, id))) / 60, 1) AS minutes_since_prev_read_at_2
    FROM with_parent_site
    WINDOW w_quality_by_quality_len AS (PARTITION BY quality ORDER BY quality_len ASC, id), w_read_at_month_by_quality_len_2 AS (PARTITION BY MONTH(read_at) ORDER BY quality_len ASC), w_quality_len AS (PARTITION BY quality_len)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY first_known_quality_len_2)) AS distinct_quality_in_first_known_quality_len_2,
        ROUND(first_known_quality_len / NULLIF(SUM(first_known_quality_len) OVER (PARTITION BY first_known_quality_len_2), 0), 4) AS first_known_quality_len_share_of_first_known_qua,
        DENSE_RANK() OVER w_all_by_distinct_site_zone_in AS distinct_site_zone_in_quality_len_dense_rank,
        LEAD(rows_in_site_id_quality_len) OVER (ORDER BY distinct_site_zone_in_quality_len, id) AS next_rows_in_site_id_quality_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(site_commissioned_on) ORDER BY quality_len, id), 3) AS quality_len_percent_rank,
        ROUND(quality_len_cume_dist / NULLIF(SUM(quality_len_cume_dist) OVER w_first_known_quality_len_2, 0), 4) AS quality_len_cume_dist_share_of_first_known_quali,
        LAG(next3_read_at_date, 2) OVER (PARTITION BY quality_len, quality_len_seq ORDER BY rows_in_site_id_quality_len, id) AS prev2_next3_read_at_date,
        MAX(kwh_avg_3_prior) OVER (PARTITION BY YEAR(site_commissioned_on) ORDER BY quality_len, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_avg_3_prior_max_1_prior
    FROM with_windows
    WINDOW w_all_by_distinct_site_zone_in AS (ORDER BY distinct_site_zone_in_quality_len, id), w_first_known_quality_len_2 AS (PARTITION BY first_known_quality_len_2)
)
-- Final projection
SELECT DISTINCT next3_read_at_date
FROM ranked
ORDER BY next3_read_at_date DESC
