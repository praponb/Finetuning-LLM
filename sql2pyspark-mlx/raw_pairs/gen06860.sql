SELECT
    id,
    store_id,
    product_id,
    last_restock,
    COALESCE(quantity, 0) AS quantity_filled,
    LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
WHERE product_id = 3 AND (last_restock < '2024-07-01' OR store_id = 5);
