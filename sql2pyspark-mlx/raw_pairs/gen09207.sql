SELECT
    id,
    product_id,
    store_id,
    last_restock,
    DATE_TRUNC('month', last_restock) AS last_restock_period,
    COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE quantity < 167
ORDER BY last_restock_period;
