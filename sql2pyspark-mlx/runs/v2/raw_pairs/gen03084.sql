SELECT
    id,
    last_restock,
    quantity,
    store_id,
    LAST_DAY(last_restock) last_restock_month_end
FROM inventory
WHERE product_id IN (1, 4, 5);
