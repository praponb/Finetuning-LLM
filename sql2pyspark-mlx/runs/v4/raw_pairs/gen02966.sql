SELECT
    quantity,
    store_id,
    product_id,
    DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock,
    ROUND(quantity * 100, 2) AS quantity_scaled
FROM inventory
