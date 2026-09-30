SELECT *, CASE WHEN quantity >= 197 THEN 'High' WHEN quantity >= 31 THEN 'Medium' ELSE 'Low' END AS quantity_level, CAST(quantity AS STRING) AS quantity_str
FROM inventory
WHERE store_id = 5
ORDER BY id DESC
LIMIT 50
