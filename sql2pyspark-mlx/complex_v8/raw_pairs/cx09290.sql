WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ABS(premium - 2289.0) AS premium_distance
    FROM policies
    WHERE status = 'ACTIVE'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY premium, id) AS rn,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        SUM(premium_distance) OVER (PARTITION BY MONTH(start_date), product ORDER BY premium_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_distance_sum_2_prior,
        MAX(premium) OVER (PARTITION BY product, MONTH(end_date)) AS product_end_date_month_max_premium,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_id, product ORDER BY premium_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(AVG(premium_distance) OVER (ORDER BY end_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_3_prior,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER (PARTITION BY product, MONTH(start_date)), 0), 4) AS premium_distance_share_of_product_start_date_mon,
        ROUND(premium_distance - AVG(premium_distance) OVER (PARTITION BY product, MONTH(end_date)), 2) AS premium_distance_vs_product_end_date_month_avg,
        FIRST_VALUE(premium_distance) IGNORE NULLS OVER (PARTITION BY holder_id, product ORDER BY premium_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_distance,
        LEAD(end_date) OVER (PARTITION BY MONTH(start_date), product ORDER BY premium_distance DESC, id) AS next_end_date,
        SUM(premium) OVER w_product_start_date_month AS product_start_date_month_sum_premium,
        ROUND(AVG(premium_distance) OVER (PARTITION BY holder_id ORDER BY premium, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_3_prior_2
    FROM scoped
    WINDOW w_product_start_date_month AS (PARTITION BY product, MONTH(start_date))
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        rn,
        premium_avg_7,
        product_end_date_month_max_premium,
        second_start_date,
        premium_distance_avg_3_prior,
        premium_distance_vs_product_end_date_month_avg,
        next_end_date,
        product_start_date_month_sum_premium,
        premium_distance_avg_3_prior_2,
        ROUND(AVG(premium_avg_7) OVER w_holder_id, 2) AS holder_id_avg_premium_avg_7,
        MIN(premium_distance_sum_2_prior) OVER (PARTITION BY MONTH(second_start_date)) AS second_start_date_month_min_premium_distance_sum,
        SUM(rn) OVER (PARTITION BY MONTH(second_start_date)) AS second_start_date_month_sum_rn,
        DENSE_RANK() OVER (ORDER BY premium_distance_avg_3_prior) AS premium_distance_avg_3_prior_dense_rank,
        LAST_VALUE(premium_distance_avg_3_prior) IGNORE NULLS OVER (ORDER BY premium_distance_share_of_product_start_date_mon, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_distance_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date) ORDER BY premium_distance_avg_3_prior DESC NULLS FIRST), 3) AS premium_distance_avg_3_prior_percent_rank,
        ROUND(premium_distance_avg_3_prior - AVG(premium_distance_avg_3_prior) OVER (PARTITION BY MONTH(second_start_date)), 2) AS premium_distance_avg_3_prior_vs_second_start_dat,
        ROUND(AVG(premium_distance_sum_2_prior) OVER (PARTITION BY MONTH(second_start_date)), 2) AS second_start_date_month_avg_premium_distance_sum
    FROM sequenced
    WINDOW w_holder_id AS (PARTITION BY holder_id)
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    premium_avg_7,
    product_end_date_month_max_premium,
    premium_distance_avg_3_prior,
    next_end_date,
    product_start_date_month_sum_premium,
    premium_distance_avg_3_prior_2,
    holder_id_avg_premium_avg_7,
    second_start_date_month_min_premium_distance_sum,
    second_start_date_month_sum_rn,
    premium_distance_avg_3_prior_dense_rank,
    last_known_premium_distance_avg_3_prior,
    premium_distance_avg_3_prior_percent_rank,
    premium_distance_avg_3_prior_vs_second_start_dat,
    second_start_date_month_avg_premium_distance_sum
FROM windowed
ORDER BY product_end_date_month_max_premium DESC, id
LIMIT 10
