WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE start_date IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        LEAD(premium) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id) AS next_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        MIN(premium) OVER (PARTITION BY holder_id) AS holder_id_min_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id, 0), 4) AS premium_share_of_holder_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, MONTH(start_date) ORDER BY start_date), 3) AS start_date_percent_rank,
        MIN(premium) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, MONTH(start_date) ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        ROUND(CUME_DIST() OVER (ORDER BY start_date, id), 3) AS start_date_cume_dist,
        ROUND(premium - AVG(premium) OVER w_holder_id, 2) AS premium_vs_holder_id_avg,
        MAX(premium) OVER (PARTITION BY holder_id) AS holder_id_max_premium,
        SUM(premium) OVER (PARTITION BY holder_id) AS holder_id_sum_premium,
        ROUND(AVG(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        NTILE(4) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id) AS end_date_quartile
    FROM prepared
    WINDOW w_holder_id AS (PARTITION BY holder_id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        holder_id_min_premium,
        premium_share_of_holder_id,
        premium_vs_holder_id_avg,
        holder_id_max_premium,
        holder_id_sum_premium,
        premium_avg_2,
        end_date_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY end_date_quartile, YEAR(start_date) ORDER BY premium_vs_holder_id_avg ASC), 3) AS premium_vs_holder_id_avg_cume_dist,
        LAG(min_premium_so_far) OVER (PARTITION BY end_date_quartile, MONTH(start_date) ORDER BY premium_share_of_holder_id, id) AS prev_min_premium_so_far,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY end_date_quartile)) AS distinct_holder_id_in_end_date_quartile,
        LAG(next_premium, 3, 0) OVER w_end_date_quartile_start_date_3 AS prev3_next_premium
    FROM windowed
    WINDOW w_end_date_quartile_start_date_3 AS (PARTITION BY end_date_quartile, YEAR(start_date) ORDER BY premium_vs_holder_id_avg ASC, id), w_end_date_quartile AS (PARTITION BY end_date_quartile)
)
SELECT
    id,
    holder_id,
    start_date,
    holder_id_max_premium,
    holder_id_sum_premium,
    premium_avg_2,
    end_date_quartile,
    premium_vs_holder_id_avg_cume_dist,
    prev3_next_premium,
    FLOOR(prev3_next_premium / 1000) * 1000 AS prev3_next_premium_bucket,
    COALESCE(prev_min_premium_so_far, 0.0) AS prev_min_premium_so_far_filled
FROM with_windows
ORDER BY prev3_next_premium DESC
