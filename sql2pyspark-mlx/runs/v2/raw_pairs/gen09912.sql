SELECT
    status,
    YEAR(order_date) AS order_year,
    SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count,
    MAX(shipping_fee) AS max_shipping_fee
FROM orders
WHERE status IN ('SHIPPED', 'PENDING', 'RETURNED') OR amount IS NULL
GROUP BY 1, 2
HAVING max_shipping_fee < 23
ORDER BY order_year DESC;
