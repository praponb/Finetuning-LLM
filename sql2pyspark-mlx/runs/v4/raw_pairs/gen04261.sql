SELECT order_date, 'order' AS source
FROM orders
UNION ALL
SELECT opened_date, 'store' AS source
FROM stores
ORDER BY order_date;
