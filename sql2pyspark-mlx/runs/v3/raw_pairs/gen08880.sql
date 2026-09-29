SELECT store_id, CASE WHEN quantity >= 251 THEN 'high' WHEN quantity >= 76 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(quantity) AS total_quantity
FROM inventory
GROUP BY CUBE(store_id, CASE WHEN quantity >= 251 THEN 'high' WHEN quantity >= 76 THEN 'mid' ELSE 'low' END)
ORDER BY store_id DESC
