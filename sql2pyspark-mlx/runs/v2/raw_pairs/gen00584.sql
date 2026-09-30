SELECT
    id,
    product_id,
    last_restock,
    LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
WHERE product_id = 2
