SELECT id, sqft, opened_date
FROM stores
WHERE region = 'West'
UNION ALL
SELECT id, sqft, opened_date
FROM stores
WHERE sqft IS NULL
ORDER BY sqft;
