SELECT
    store_id,
    MAX(quantity) AS max_quantity,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    COUNT(*) AS inventory_count
FROM inventory
WHERE last_restock >= '2024-05-01'
GROUP BY store_id
ORDER BY max_quantity DESC;
