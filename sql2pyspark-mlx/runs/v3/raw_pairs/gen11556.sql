SELECT id, region, city
FROM stores
WHERE opened_date >= '2025-04-18'
UNION
SELECT id, region, city
FROM stores
WHERE city IN ('Berlin', 'Chiang Mai');
