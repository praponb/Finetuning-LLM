SELECT region, opened_date
FROM stores
WHERE store_name LIKE 'A%'
UNION
SELECT region, opened_date
FROM stores
WHERE store_name IS NOT NULL;
