SELECT
    category,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    STDDEV_SAMP(price) AS price_stddev,
    CORR(stock_qty, cost) AS corr_stock_qty_cost,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) AS cost_per_grocery
FROM products
GROUP BY 1, 2;
