SELECT
    store_id,
    DATE_TRUNC('MONTH', last_restock) AS last_restock_month,
    COUNT(*) AS num_inventory,
    COUNT(quantity) AS quantity_count
FROM inventory
GROUP BY store_id, DATE_TRUNC('MONTH', last_restock)
HAVING COUNT(quantity) >= 1
ORDER BY quantity_count DESC, num_inventory DESC, store_id, last_restock_month
LIMIT 10
