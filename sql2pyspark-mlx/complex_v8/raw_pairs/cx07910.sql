WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 3521.8) AS premium_distance
    FROM policies
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC NULLS FIRST, id)) AS days_since_prev_end_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC NULLS FIRST), 3) AS start_date_percent_rank,
        NTH_VALUE(premium_distance, 2) OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_distance,
        DENSE_RANK() OVER w_holder_id_start_date_year_by_3 AS start_date_dense_rank,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER (PARTITION BY status, product), 0), 4) AS premium_distance_share_of_status_product,
        ROUND(AVG(premium_distance) OVER (PARTITION BY status, product ORDER BY end_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_distance_avg_2,
        SUM(premium_distance) OVER w_status_product AS status_product_sum_premium_distance,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status, product ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(AVG(premium_distance) OVER (PARTITION BY product ORDER BY premium_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_2_prior,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status, product), 2) AS premium_vs_status_product_avg,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status, product)) AS distinct_holder_id_in_status_product,
        RANK() OVER (PARTITION BY status, product ORDER BY end_date) AS end_date_rank
    FROM prepared
    WINDOW w_holder_id_start_date_year_by_3 AS (PARTITION BY holder_id, YEAR(start_date) ORDER BY start_date DESC NULLS FIRST), w_status_product AS (PARTITION BY status, product)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(premium) OVER w_end_date_year AS end_date_year_sum_premium,
        SUM(start_date_percent_rank) OVER (PARTITION BY product ORDER BY start_date NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS start_date_percent_rank_sum_1_prior,
        LAG(end_date_rank, 3, 0) OVER (ORDER BY running_premium DESC, id) AS prev3_end_date_rank,
        SUM(end_date_rank) OVER w_end_date_year AS end_date_year_sum_end_date_rank,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY YEAR(end_date))) AS distinct_holder_id_in_end_date_year
    FROM windowed
    WINDOW w_end_date_year AS (PARTITION BY YEAR(end_date))
)
-- Final projection
SELECT
    id,
    product,
    premium,
    start_date,
    end_date,
    status,
    premium_distance,
    running_premium,
    days_since_prev_end_date,
    start_date_percent_rank,
    start_date_dense_rank,
    premium_distance_avg_2,
    premium_distance_avg_2_prior,
    premium_vs_status_product_avg,
    distinct_holder_id_in_status_product,
    end_date_rank,
    end_date_year_sum_premium,
    start_date_percent_rank_sum_1_prior,
    end_date_year_sum_end_date_rank,
    distinct_holder_id_in_end_date_year,
    FLOOR(second_premium_distance / 1000) * 1000 AS second_premium_distance_bucket,
    FLOOR(end_date_rank / 2) * 2 AS end_date_rank_bucket
FROM ranked
ORDER BY distinct_holder_id_in_end_date_year DESC, premium_distance_avg_2_prior NULLS LAST
