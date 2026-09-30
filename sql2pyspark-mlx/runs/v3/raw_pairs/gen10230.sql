SELECT sale_date
FROM store_sales
WHERE units IS NOT NULL
UNION ALL
SELECT sale_date
FROM store_sales
WHERE store_id IN (3, 4, 5, 6)
ORDER BY sale_date
