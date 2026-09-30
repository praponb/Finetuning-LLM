SELECT CASE WHEN quantity >= 182 THEN 'high' WHEN quantity >= 78 THEN 'mid' ELSE 'low' END AS quantity_bucket, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE quantity > 184 AND last_restock <= '2025-03-01'
GROUP BY CASE WHEN quantity >= 182 THEN 'high' WHEN quantity >= 78 THEN 'mid' ELSE 'low' END
