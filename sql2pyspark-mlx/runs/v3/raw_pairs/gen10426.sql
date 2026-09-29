SELECT opened_date
FROM stores
INTERSECT
SELECT sale_date
FROM store_sales
WHERE store_id IN (2, 3, 5, 6);
