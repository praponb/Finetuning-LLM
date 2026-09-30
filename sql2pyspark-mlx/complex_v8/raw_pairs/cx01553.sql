WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product <> 'HOME'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_by_start_date) AS days_since_prev_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_start_date_month, 0), 4) AS premium_share_of_product_start_date_month,
        NTILE(4) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date, id) AS start_date_quartile,
        LAG(premium, 2) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date, id) AS prev2_premium,
        MAX(premium) OVER (PARTITION BY YEAR(start_date), status ORDER BY start_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_max_3_prior,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, DATE_TRUNC('MONTH', start_date))) AS distinct_holder_id_in_product_start_date_month,
        FIRST_VALUE(premium) OVER (PARTITION BY product, status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        SUM(premium) OVER (PARTITION BY product, DATE_TRUNC('MONTH', start_date)) AS product_start_date_month_sum_premium,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(start_date), status ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        COUNT(*) OVER (PARTITION BY product, DATE_TRUNC('MONTH', start_date)) AS rows_in_product_start_date_month,
        MAX(premium) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_max_1_prior,
        MIN(premium) OVER w_product_start_date_month AS product_start_date_month_min_premium
    FROM base
    WINDOW w_status_by_start_date AS (PARTITION BY status ORDER BY start_date ASC, id), w_product_start_date_month AS (PARTITION BY product, DATE_TRUNC('MONTH', start_date))
)
SELECT
    id,
    start_date,
    end_date,
    status,
    days_since_prev_start_date,
    premium_share_of_product_start_date_month,
    start_date_quartile,
    prev2_premium,
    premium_max_3_prior,
    first_premium,
    product_start_date_month_sum_premium,
    premium_avg_6_prior,
    rows_in_product_start_date_month,
    premium_max_1_prior,
    product_start_date_month_min_premium,
    premium_max_1_prior - prev2_premium AS premium_max_1_prior_minus_prev2_premium
FROM metrics
WHERE product IN ('AUTO', 'LIFE', 'TRAVEL')
ORDER BY days_since_prev_start_date DESC, product_start_date_month_min_premium
