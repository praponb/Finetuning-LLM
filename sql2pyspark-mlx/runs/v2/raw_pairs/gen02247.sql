SELECT
    i.id,
    i.last_restock,
    ss.store_id,
    ss.units,
    ss.product_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id = 9 OR i.store_id IN (2, 3, 5, 6)
ORDER BY id DESC
