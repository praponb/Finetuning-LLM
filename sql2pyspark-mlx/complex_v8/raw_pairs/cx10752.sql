WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 0.9, 2) AS premium_adj,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM policies
    WHERE product IS NOT NULL
      AND start_date >= DATE '2024-10-22'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product, holder_id)) AS distinct_status_in_product_holder_id,
        NTILE(3) OVER (PARTITION BY holder_id ORDER BY premium ASC, id) AS premium_tercile,
        DENSE_RANK() OVER (ORDER BY end_date DESC) AS end_date_dense_rank,
        RANK() OVER (PARTITION BY holder_id ORDER BY premium ASC, id) AS premium_rank,
        premium_adj - LAG(premium_adj) OVER (PARTITION BY holder_id ORDER BY premium DESC, id) AS premium_adj_change,
        LAG(start_date, 2) OVER (PARTITION BY holder_id ORDER BY premium ASC, id) AS prev2_start_date,
        MAX(premium) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        MAX(premium_adj) OVER (ORDER BY end_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_adj_max_7,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS premium_vs_product_holder_id_avg,
        ROUND(AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS product_holder_id_avg_premium,
        ROUND(premium_adj / NULLIF(SUM(premium_adj) OVER (PARTITION BY product, holder_id), 0), 4) AS premium_adj_share_of_product_holder_id
    FROM base_policys
),
totals AS (
    -- Summarise per group
    SELECT
        YEAR(prev2_start_date) AS prev2_start_date_year,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        SUM(premium) FILTER (WHERE product <> 'AUTO') AS premium_product_auto,
        ROUND(SUM(max_premium_so_far), 2) AS total_max_premium_so_far,
        ROUND(STDDEV(premium_adj_max_7), 2) AS stddev_premium_adj_max_7
    FROM with_windows
    GROUP BY YEAR(prev2_start_date), DATE_TRUNC('MONTH', end_date)
)
-- Final projection
SELECT
    prev2_start_date_year,
    end_date_month,
    premium_product_auto,
    total_max_premium_so_far,
    stddev_premium_adj_max_7,
    prev2_start_date_year - stddev_premium_adj_max_7 AS prev2_start_date_year_minus_stddev_premium_adj_m
FROM totals
ORDER BY prev2_start_date_year DESC, premium_product_auto, end_date_month
LIMIT 10
