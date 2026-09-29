SELECT status, store_id
FROM orders
WHERE channel = 'store'
UNION
SELECT status, store_id
FROM orders
WHERE customer_id = 30
