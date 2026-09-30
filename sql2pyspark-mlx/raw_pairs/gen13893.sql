SELECT store_id, last_restock, CASE WHEN quantity >= 217 THEN 'High' WHEN quantity >= 32 THEN 'Medium' ELSE 'Low' END quantity_band
FROM inventory
ORDER BY store_id DESC
