SELECT amount
FROM orders
WHERE amount IS NOT NULL
EXCEPT
SELECT price AS amount
FROM products
WHERE price IS NOT NULL;
