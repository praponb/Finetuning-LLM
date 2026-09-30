SELECT product_id, COALESCE(quantity, 0) AS quantity_filled, CASE WHEN quantity >= 257 THEN 'High' WHEN quantity >= 53 THEN 'Medium' ELSE 'Low' END AS quantity_band
FROM inventory
WHERE store_id IN (1, 3, 5)
