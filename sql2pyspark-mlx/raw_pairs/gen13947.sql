SELECT
    id,
    store_id,
    last_restock,
    quantity,
    quantity - LAG(quantity, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS quantity_change
FROM inventory
ORDER BY quantity DESC;
