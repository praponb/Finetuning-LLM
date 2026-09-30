SELECT store_id, sale_date
FROM store_sales
WHERE store_id = 2
INTERSECT
SELECT store_id, sale_date
FROM store_sales
WHERE revenue IS NOT NULL
