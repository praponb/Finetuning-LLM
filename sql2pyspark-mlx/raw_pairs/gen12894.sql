SELECT quantity_bucket, max_quantity, row_count
FROM (
    SELECT CASE WHEN quantity >= 237 THEN 'high' WHEN quantity >= 90 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(quantity) AS max_quantity, COUNT(*) AS row_count
    FROM inventory
    GROUP BY CASE WHEN quantity >= 237 THEN 'high' WHEN quantity >= 90 THEN 'mid' ELSE 'low' END
) AS sub
WHERE max_quantity > 180
