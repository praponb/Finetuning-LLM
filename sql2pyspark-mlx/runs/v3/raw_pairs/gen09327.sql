SELECT
    id,
    store_id,
    quantity,
    MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity
FROM inventory
ORDER BY store_id DESC, quantity DESC
