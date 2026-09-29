SELECT units, product_id
FROM store_sales
WHERE store_id IN (1, 3, 5, 6)
UNION
SELECT units, product_id
FROM store_sales
WHERE revenue <= 2800
