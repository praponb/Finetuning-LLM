SELECT store_id
FROM orders
WHERE customer_id IN (9, 13, 27)
UNION ALL
SELECT store_id
FROM orders
WHERE order_date >= '2024-05-01';
