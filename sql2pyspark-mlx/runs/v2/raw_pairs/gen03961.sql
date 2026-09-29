SELECT
    store_id,
    product_id,
    MONTH(last_restock) AS last_restock_month,
    COUNT(*) AS cnt,
    MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY 1, 2, 3
ORDER BY quantity_range DESC, store_id ASC, product_id ASC, last_restock_month ASC
LIMIT 50;
