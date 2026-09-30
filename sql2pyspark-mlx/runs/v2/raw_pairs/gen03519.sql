SELECT
    id,
    product_id,
    store_id,
    LAST_DAY(last_restock) last_restock_month_end
FROM inventory
WHERE last_restock > '2024-10-28'
