SELECT region, sqft
FROM stores
WHERE opened_date BETWEEN '2024-04-01' AND '2025-05-29'
UNION
SELECT region, sqft
FROM stores
WHERE sqft <= 15441
ORDER BY region
