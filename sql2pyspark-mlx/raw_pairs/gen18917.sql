SELECT sale_date
FROM store_sales
WHERE store_id = 5
INTERSECT
SELECT sale_date
FROM store_sales
WHERE store_id IN (2, 3, 4);
