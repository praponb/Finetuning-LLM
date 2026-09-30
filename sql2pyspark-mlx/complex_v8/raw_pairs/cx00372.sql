WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM policies
    WHERE YEAR(start_date) = 2025
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        premium,
        start_date,
        status,
        start_date_label,
        MAX(premium) OVER (PARTITION BY product) AS product_max_premium,
        MAX(premium) OVER w_start_date_month_product AS start_date_month_product_max_premium,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date), 3) AS start_date_percent_rank,
        LAST_VALUE(start_date) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        ROUND(AVG(premium) OVER (PARTITION BY MONTH(start_date), product), 2) AS start_date_month_product_avg_premium,
        ROUND(AVG(premium) OVER (ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(start_date), product)) AS distinct_status_in_start_date_month_product,
        DENSE_RANK() OVER (PARTITION BY product, status ORDER BY premium) AS premium_dense_rank,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM filtered_policys
    WINDOW w_start_date_month_product AS (PARTITION BY MONTH(start_date), product)
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        premium,
        start_date,
        start_date_label,
        product_max_premium,
        start_date_month_product_max_premium,
        start_date_percent_rank,
        last_start_date,
        start_date_month_product_avg_premium,
        premium_avg_6_prior,
        distinct_status_in_start_date_month_product,
        premium_dense_rank,
        last_known_start_date,
        ROUND(CUME_DIST() OVER w_status_by_premium_share_of_p, 3) AS premium_share_of_product_cume_dist,
        LAST_VALUE(premium_dense_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY premium_share_of_product DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_dense_rank,
        LAG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status ORDER BY premium_share_of_product DESC, id) AS prev_premium,
        MAX(premium_dense_rank) OVER w_product AS product_max_premium_dense_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product)) AS distinct_status_in_product,
        ROUND(premium_share_of_product - AVG(premium_share_of_product) OVER w_product, 2) AS premium_share_of_product_vs_product_avg
    FROM ranked
    WINDOW w_status_by_premium_share_of_p AS (PARTITION BY status ORDER BY premium_share_of_product DESC NULLS FIRST), w_product AS (PARTITION BY product)
)
-- Final projection
SELECT
    id,
    product,
    premium,
    start_date,
    start_date_label,
    product_max_premium,
    start_date_month_product_max_premium,
    last_start_date,
    premium_avg_6_prior,
    last_known_start_date,
    premium_share_of_product_cume_dist,
    last_premium_dense_rank,
    prev_premium,
    DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label_2,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown
FROM ranked_2
ORDER BY last_known_start_date NULLS LAST
