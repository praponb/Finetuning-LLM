SELECT order_date, 'order' AS source
FROM orders
UNION
SELECT opened_date AS order_date, 'store' AS source
FROM stores
