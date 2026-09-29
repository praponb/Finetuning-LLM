SELECT
    store_id,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    MIN(last_restock) AS min_last_restock,
    MAX(quantity) AS max_quantity
FROM inventory
GROUP BY store_id
ORDER BY min_last_restock
