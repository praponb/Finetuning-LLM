SELECT id, store_id, order_date
FROM orders
WHERE channel IS NULL
EXCEPT
SELECT id, store_id, order_date
FROM orders
WHERE customer_id = 4
