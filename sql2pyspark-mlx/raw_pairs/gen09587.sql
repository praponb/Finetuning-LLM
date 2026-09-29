SELECT launch_date event_date, 'product' kind
FROM products
WHERE launch_date BETWEEN '2024-04-01' AND '2025-09-01'
UNION ALL
SELECT order_date event_date, 'order' kind
FROM orders;
