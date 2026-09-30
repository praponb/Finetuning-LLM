SELECT
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_month,
    status,
    COUNT(DISTINCT shipping_fee) AS unique_channels,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
FROM orders
GROUP BY 1, 2
ORDER BY order_month DESC;
