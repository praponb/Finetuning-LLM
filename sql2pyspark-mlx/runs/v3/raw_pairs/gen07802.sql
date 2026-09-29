SELECT
    DATE_TRUNC('MONTH', order_date) AS order_month,
    COUNT(*) AS order_count,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count,
    MAX(amount) - MIN(amount) AS amount_range
FROM orders
GROUP BY order_month
HAVING amount_range >= 59
ORDER BY order_month;
