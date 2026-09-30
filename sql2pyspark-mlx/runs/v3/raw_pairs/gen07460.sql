SELECT
    id,
    store_id,
    last_restock,
    product_id,
    LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
WHERE product_id IN (3, 4, 6, 10)
