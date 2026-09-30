SELECT
    id,
    product_id,
    store_id,
    last_restock,
    quantity,
    LAG(quantity, 1, 0) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) AS prev_quantity,
    LEAD(quantity, 1, 0) OVER (PARTITION BY YEAR(last_restock) ORDER BY last_restock, id) AS next_quantity
FROM inventory
ORDER BY last_restock
