SELECT store_id, units
FROM store_sales
WHERE store_id IN (2, 3, 4, 6)
UNION ALL
SELECT store_id, units
FROM store_sales
WHERE sale_date < '2024-09-01'
ORDER BY units
