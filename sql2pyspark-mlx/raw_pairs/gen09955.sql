SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    APPROX_PERCENTILE(price, 0.95) AS price_p95,
    SUM(stock_qty) FILTER (WHERE category = 'Sports') AS sports_total_stock_qty
FROM products
GROUP BY week_start
HAVING APPROX_PERCENTILE(price, 0.95) >= 0
ORDER BY sports_total_stock_qty DESC NULLS FIRST
