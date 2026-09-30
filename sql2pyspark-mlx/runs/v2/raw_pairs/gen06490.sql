SELECT opened_date
FROM stores
WHERE store_name LIKE 'A%'
UNION ALL
SELECT opened_date
FROM stores
WHERE region != 'Central'
ORDER BY opened_date;
