SELECT
    id,
    quantity,
    product_id,
    last_restock,
    ROUND(quantity, 1) AS quantity_rounded
FROM inventory
