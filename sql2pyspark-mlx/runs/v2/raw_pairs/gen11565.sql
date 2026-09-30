SELECT
    product_id,
    store_id,
    last_restock,
    CASE WHEN quantity >= 246 THEN 'High' WHEN quantity >= 116 THEN 'Medium' ELSE 'Low' END quantity_band,
    CASE WHEN quantity >= 219 THEN 'High' WHEN quantity >= 58 THEN 'Medium' ELSE 'Low' END quantity_band_2
FROM inventory
WHERE quantity > 159;
