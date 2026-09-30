WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-07-07'
      AND holder_id <= 16
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(premium) OVER (ORDER BY start_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_2_prior,
        MAX(premium) OVER (PARTITION BY status) AS status_max_premium,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY end_date, id) AS rn,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY YEAR(end_date))) AS distinct_status_in_end_date_year,
        ROUND(AVG(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior,
        LEAD(premium, 1, 0) OVER w_product_holder_id_by_start_d AS next_premium,
        LAG(end_date, 2) OVER w_all_by_start_date AS prev2_end_date,
        MIN(premium) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        SUM(premium) OVER (PARTITION BY product, status ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        COUNT(*) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        RANK() OVER (ORDER BY start_date DESC) AS start_date_rank
    FROM scoped
    WINDOW w_product_holder_id_by_start_d AS (PARTITION BY product, holder_id ORDER BY start_date, id), w_all_by_start_date AS (ORDER BY start_date ASC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status,
    premium_avg_2_prior,
    rn,
    distinct_status_in_end_date_year,
    premium_avg_3_prior,
    next_premium,
    min_premium_so_far,
    premium_sum_3_prior,
    running_count,
    start_date_percent_rank,
    start_date_rank
FROM sequenced
WHERE premium_avg_2_prior < 3560.0
ORDER BY premium_avg_3_prior DESC NULLS FIRST, status
