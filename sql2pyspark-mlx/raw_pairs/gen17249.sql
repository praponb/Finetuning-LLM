SELECT city, region
FROM stores
EXCEPT DISTINCT
SELECT city, region
FROM stores
WHERE city IN ('Bangkok', 'Singapore', 'London');
