SELECT units
FROM store_sales
WHERE revenue < 1300
UNION
SELECT units
FROM store_sales
WHERE store_id = 2;
