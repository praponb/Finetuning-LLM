SELECT price
FROM products
WHERE supplier_id IN (24, 32)
UNION
SELECT amount
FROM orders
WHERE customer_id = 1;
