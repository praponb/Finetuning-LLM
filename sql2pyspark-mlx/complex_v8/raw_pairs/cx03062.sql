WITH scoped_policys AS (
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
    WHERE holder_id IS NOT NULL
      AND start_date IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        start_date,
        end_date,
        status,
        DENSE_RANK() OVER (PARTITION BY MONTH(start_date), DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC) AS start_date_dense_rank,
        RANK() OVER (PARTITION BY product, status ORDER BY start_date, id) AS start_date_rank,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product), 2) AS premium_vs_product_avg,
        ROUND(AVG(premium) OVER (PARTITION BY MONTH(end_date) ORDER BY start_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        MAX(premium) OVER (PARTITION BY MONTH(start_date), DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_max_3,
        LAG(start_date, 3) OVER (PARTITION BY MONTH(end_date) ORDER BY start_date ASC, id) AS prev3_start_date,
        MIN(premium) OVER w_product AS product_min_premium,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY MONTH(start_date), DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product)) AS distinct_status_in_product
    FROM scoped_policys
    WINDOW w_product AS (PARTITION BY product)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY status, YEAR(second_end_date) ORDER BY start_date_rank), 3) AS start_date_rank_cume_dist,
        ROUND(start_date_dense_rank / NULLIF(SUM(start_date_dense_rank) OVER (PARTITION BY MONTH(end_date)), 0), 4) AS start_date_dense_rank_share_of_end_date_month,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY MONTH(end_date))) AS distinct_product_in_end_date_month,
        ROUND(AVG(distinct_status_in_product) OVER (PARTITION BY MONTH(end_date)), 2) AS end_date_month_avg_distinct_status_in_product,
        ROUND(premium_vs_product_avg / NULLIF(SUM(premium_vs_product_avg) OVER w_end_date_month, 0), 4) AS premium_vs_product_avg_share_of_end_date_month,
        SUM(premium_avg_4) OVER (PARTITION BY MONTH(end_date)) AS end_date_month_sum_premium_avg_4,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(end_date))) AS distinct_status_in_end_date_month,
        NTH_VALUE(premium_max_3, 2) OVER (PARTITION BY status, YEAR(second_end_date) ORDER BY start_date_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_max_3
    FROM with_trends
    WINDOW w_end_date_month AS (PARTITION BY MONTH(end_date))
)
SELECT
    id,
    product,
    start_date,
    status,
    start_date_dense_rank,
    start_date_rank,
    premium_avg_4,
    premium_max_3,
    prev3_start_date,
    product_min_premium,
    distinct_status_in_product,
    start_date_rank_cume_dist,
    start_date_dense_rank_share_of_end_date_month,
    distinct_product_in_end_date_month,
    end_date_month_avg_distinct_status_in_product,
    end_date_month_sum_premium_avg_4,
    distinct_status_in_end_date_month,
    LENGTH(TRIM(product)) AS product_len
FROM windowed
ORDER BY distinct_product_in_end_date_month NULLS LAST, premium_max_3 DESC
