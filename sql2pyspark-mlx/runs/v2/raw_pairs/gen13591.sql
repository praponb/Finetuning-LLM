SELECT
    store_id,
    product_id,
    MAX(last_restock) AS latest_last_restock,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE product_id IN (1, 2, 3, 6) AND store_id IN (3, 6)
GROUP BY store_id, product_id
ORDER BY 3, product_id DESC, store_id
LIMIT 1
