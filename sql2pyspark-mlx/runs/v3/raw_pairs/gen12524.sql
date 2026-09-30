SELECT id, customer_id, order_date
FROM orders
WHERE store_id IS NULL
UNION ALL
SELECT id, customer_id, order_date
FROM orders
WHERE amount < 1330;
