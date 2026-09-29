SELECT CASE WHEN quantity >= 267 THEN 'high' WHEN quantity >= 32 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(DISTINCT quantity) AS unique_stores, COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE last_restock >= '2024-03-01'
GROUP BY CASE WHEN quantity >= 267 THEN 'high' WHEN quantity >= 32 THEN 'mid' ELSE 'low' END
HAVING COUNT(DISTINCT quantity) >= 2;
