SELECT
    id,
    store_id,
    last_restock,
    quantity,
    FIRST_VALUE(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS first_quantity
FROM inventory
