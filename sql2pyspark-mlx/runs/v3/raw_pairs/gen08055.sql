SELECT id, region, store_name
FROM stores
WHERE opened_date BETWEEN '2025-02-01' AND '2025-07-01'
UNION ALL
SELECT id, region, store_name
FROM stores
WHERE opened_date < '2024-09-08';
