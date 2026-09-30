WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        CONCAT_WS(': ', product, status) AS product_status_label
    FROM policies
    WHERE holder_id IS NOT NULL
      AND product <> 'AUTO'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(premium) OVER (PARTITION BY product) AS product_min_premium,
        COUNT(*) OVER w_end_date_month AS rows_in_end_date_month,
        ROUND(AVG(premium) OVER (PARTITION BY product), 2) AS product_avg_premium,
        NTILE(3) OVER (PARTITION BY product ORDER BY premium DESC NULLS FIRST, id) AS premium_tercile,
        LAST_VALUE(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product)) AS distinct_status_in_product,
        ROUND(AVG(premium) OVER (PARTITION BY product ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        LAST_VALUE(premium) OVER (PARTITION BY YEAR(end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_2
    FROM scoped_policys
    WINDOW w_end_date_month AS (PARTITION BY MONTH(end_date)), w_product AS (PARTITION BY product)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DATEDIFF(start_date, LAG(start_date) OVER w_distinct_status_in_product_b) AS days_since_prev_start_date,
        ROUND(product_min_premium - AVG(product_min_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product), 2) AS product_min_premium_vs_start_date_month_product_,
        NTILE(10) OVER (ORDER BY end_date, id) AS end_date_decile,
        SIZE(COLLECT_SET(holder_id) OVER w_start_date_month_product) AS distinct_holder_id_in_start_date_month_product,
        ROUND(premium - AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product), 2) AS premium_vs_start_date_month_product_avg
    FROM metrics
    WINDOW w_distinct_status_in_product_b AS (PARTITION BY distinct_status_in_product ORDER BY last_premium_2 ASC, id), w_start_date_month_product AS (PARTITION BY DATE_TRUNC('MONTH', start_date), product)
)
-- Final projection
SELECT
    id,
    product,
    premium,
    start_date,
    status,
    product_min_premium,
    rows_in_end_date_month,
    product_avg_premium,
    premium_share_of_product,
    distinct_status_in_product,
    premium_avg_4,
    last_premium_2,
    days_since_prev_start_date,
    end_date_decile,
    distinct_holder_id_in_start_date_month_product,
    premium_vs_start_date_month_product_avg,
    LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
    premium_share_of_product + last_premium_2 AS premium_share_of_product_plus_last_premium_2
FROM windowed
