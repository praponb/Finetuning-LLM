SELECT region, sqft
FROM stores
WHERE opened_date >= '2024-08-01'
UNION ALL
SELECT region, sqft
FROM stores
WHERE sqft BETWEEN 7737 AND 12200;
