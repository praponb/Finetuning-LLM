SELECT price
FROM products
EXCEPT
SELECT cost
FROM shipments
WHERE order_id = 20
ORDER BY price DESC;
