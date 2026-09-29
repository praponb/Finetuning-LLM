SELECT order_month, stddev_shipping_fee, pct_cancelled
FROM (
    SELECT MONTH(order_date) AS order_month, ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled
    FROM orders
    GROUP BY MONTH(order_date)
) AS s
WHERE stddev_shipping_fee < 26
ORDER BY order_month
LIMIT 5;
