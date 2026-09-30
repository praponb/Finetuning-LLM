SELECT id, order_date
FROM orders
WHERE store_id IN (22, 23, 28, 39)
EXCEPT
SELECT id, order_date
FROM orders
WHERE shipping_fee BETWEEN 23 AND 35
ORDER BY order_date;
