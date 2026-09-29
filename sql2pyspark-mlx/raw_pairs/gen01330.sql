SELECT order_month, store_count
FROM (
    SELECT MONTH(order_date) AS order_month, SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) AS store_count
    FROM orders
    WHERE channel = 'mobile'
    GROUP BY MONTH(order_date)
) AS s
WHERE store_count < 47
ORDER BY order_month DESC;
