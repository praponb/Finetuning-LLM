SELECT id, sqft, opened_date
FROM stores
WHERE region = 'West'
UNION ALL
SELECT id, sqft, opened_date
FROM stores
WHERE YEAR(opened_date) >= 2025
ORDER BY opened_date
