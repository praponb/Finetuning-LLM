SELECT product_id
FROM store_sales
WHERE revenue > 3600
EXCEPT
SELECT product_id
FROM store_sales
WHERE product_id IN (4, 7, 8, 10)
ORDER BY product_id DESC;
