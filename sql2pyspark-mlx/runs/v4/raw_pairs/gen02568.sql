SELECT
    id,
    store_id,
    quantity,
    DATE_TRUNC('year', last_restock) AS last_restock_period,
    CASE WHEN quantity >= 185 THEN 'High' WHEN quantity >= 103 THEN 'Medium' ELSE 'Low' END AS quantity_tier
FROM inventory
WHERE store_id IN (1, 2, 4, 6) AND (store_id IN (2, 4, 6) OR product_id = 1);
