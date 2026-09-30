SELECT *
FROM orders
WHERE order_date < '2024-03-06' AND store_id IN (6, 24, 35)
ORDER BY status;
