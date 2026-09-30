WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE start_date BETWEEN DATE '2024-03-27' AND DATE '2025-03-23'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product, YEAR(end_date)), 2) AS premium_vs_product_end_date_year_avg,
        MAX(premium) OVER w_product_or_unknown_end_date AS product_or_unknown_end_date_month_max_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product_or_unknown, DATE_TRUNC('MONTH', end_date)), 0), 4) AS premium_share_of_product_or_unknown_end_date_mon,
        ROUND(AVG(premium) OVER (PARTITION BY product_or_unknown ORDER BY premium, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_2_prior,
        ROUND(AVG(premium) OVER (PARTITION BY product, YEAR(end_date)), 2) AS product_end_date_year_avg_premium,
        ROUND(AVG(premium) OVER w_product_or_unknown_end_date, 2) AS product_or_unknown_end_date_month_avg_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY product_or_unknown ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_end_date_year, 0), 4) AS premium_share_of_product_end_date_year,
        LAG(premium, 2, 0) OVER (PARTITION BY product_or_unknown ORDER BY premium, id) AS prev2_premium
    FROM cleaned
    WINDOW w_product_or_unknown_end_date AS (PARTITION BY product_or_unknown, DATE_TRUNC('MONTH', end_date)), w_product_end_date_year AS (PARTITION BY product, YEAR(end_date))
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        product_or_unknown,
        premium_vs_product_end_date_year_avg,
        product_or_unknown_end_date_month_max_premium,
        premium_share_of_product_or_unknown_end_date_mon,
        premium_avg_2_prior,
        product_end_date_year_avg_premium,
        second_premium,
        premium_share_of_product_end_date_year,
        prev2_premium,
        COUNT(*) OVER w_product_or_unknown AS rows_in_product_or_unknown,
        RANK() OVER (ORDER BY premium DESC, id) AS premium_rank,
        MAX(prev2_premium) OVER (PARTITION BY holder_id ORDER BY premium_avg_2_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prev2_premium_so_far,
        NTILE(10) OVER (PARTITION BY holder_id ORDER BY premium_avg_2_prior ASC, id) AS premium_avg_2_prior_decile,
        ROUND(AVG(premium_avg_2_prior) OVER (PARTITION BY product_or_unknown), 2) AS product_or_unknown_avg_premium_avg_2_prior
    FROM with_trends
    WINDOW w_product_or_unknown AS (PARTITION BY product_or_unknown)
),
totals AS (
    -- Group-level totals
    SELECT
        holder_id,
        SUM(max_prev2_premium_so_far) AS total_max_prev2_premium_so_far,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(premium_share_of_product_end_date_year) AS max_premium_share_of_product_end_date_year,
        COUNT(*) AS cnt
    FROM metrics
    WHERE rows_in_product_or_unknown <= 15
    GROUP BY holder_id
)
-- Final projection
SELECT
    holder_id,
    total_max_prev2_premium_so_far,
    distinct_ids,
    max_premium_share_of_product_end_date_year,
    cnt
FROM totals
ORDER BY max_premium_share_of_product_end_date_year, cnt DESC NULLS FIRST
