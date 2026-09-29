SELECT
    DATE_TRUNC('MONTH', launch_date) AS launch_month,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(*) AS cnt,
    SUM(SUM(stock_qty)) OVER (ORDER BY DATE_TRUNC('MONTH', launch_date)) AS cumulative_stock_qty
FROM products
WHERE ARRAY_CONTAINS(tags, 'clearance')
GROUP BY DATE_TRUNC('MONTH', launch_date)
ORDER BY cumulative_stock_qty DESC;
