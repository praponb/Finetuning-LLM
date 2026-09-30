SELECT id, sale_date, store_id
FROM store_sales
WHERE revenue BETWEEN 3500 AND 3900
UNION
SELECT id, sale_date, store_id
FROM store_sales
WHERE store_id = 4
ORDER BY id
