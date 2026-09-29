SELECT product_id, CASE WHEN quantity >= 269 THEN 'High' WHEN quantity >= 42 THEN 'Medium' ELSE 'Low' END AS quantity_band
FROM inventory
