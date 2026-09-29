SELECT quantity_bucket, unique_products
FROM (
    SELECT CASE WHEN quantity >= 210 THEN 'high' WHEN quantity >= 48 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    GROUP BY CASE WHEN quantity >= 210 THEN 'high' WHEN quantity >= 48 THEN 'mid' ELSE 'low' END
) AS s
WHERE unique_products >= 5
ORDER BY unique_products DESC
