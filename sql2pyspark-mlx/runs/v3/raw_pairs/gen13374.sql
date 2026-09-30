SELECT price AS value
FROM products
WHERE cost >= 420
UNION
SELECT revenue AS value
FROM store_sales
WHERE store_id = 2
