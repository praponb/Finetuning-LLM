SELECT
    id,
    store_id,
    last_restock,
    quantity,
    SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
FROM inventory
ORDER BY running_quantity DESC
