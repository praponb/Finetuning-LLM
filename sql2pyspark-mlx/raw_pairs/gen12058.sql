SELECT CASE WHEN quantity >= 248 THEN 'high' WHEN quantity >= 89 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(last_restock) AS latest_last_restock
FROM inventory
GROUP BY 1
ORDER BY quantity_bucket
