SELECT
    id,
    product_id,
    store_id,
    CASE WHEN quantity >= 232 THEN 'High' WHEN quantity >= 74 THEN 'Medium' ELSE 'Low' END AS quantity_band
FROM inventory
WHERE last_restock <= '2025-06-01'
ORDER BY store_id DESC, quantity_band;
