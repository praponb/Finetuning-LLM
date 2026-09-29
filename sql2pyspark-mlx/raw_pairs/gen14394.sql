SELECT
    ss.store_id,
    ss.product_id,
    MIN(i.last_restock) AS first_last_restock,
    MAX(i.last_restock) AS latest_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (2, 3, 5)
GROUP BY ss.store_id, ss.product_id
ORDER BY latest_last_restock
