SELECT
    id,
    product_id,
    DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label,
    CASE WHEN quantity >= 181 THEN 'High' WHEN quantity >= 100 THEN 'Medium' ELSE 'Low' END AS quantity_band
FROM inventory
WHERE product_id IN (4, 5) AND product_id IN (5, 7)
