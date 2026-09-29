SELECT
    ss.store_id,
    ss.product_id,
    MAX(i.last_restock) AS latest_last_restock,
    COUNT(*) AS total_count
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id, ss.product_id
HAVING COUNT(*) >= 4;
