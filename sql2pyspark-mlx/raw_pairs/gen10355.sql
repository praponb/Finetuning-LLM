SELECT
    *,
    MIN(price) OVER w_running AS brand_min_price,
    SUM(price) OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), supplier_id) AS launch_date_period_sum_price,
    ROUND(price / SUM(price) OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date)), 4) AS price_share,
    AVG(price) OVER (PARTITION BY YEAR(launch_date), brand) AS launch_date_year_avg_price
FROM products
WINDOW w_running AS (PARTITION BY brand, category)
