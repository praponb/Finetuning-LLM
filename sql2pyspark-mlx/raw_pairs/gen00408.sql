SELECT store_id, product_id
FROM store_sales
INTERSECT
SELECT store_id, product_id
FROM store_sales
WHERE product_id IN (8, 10)
