SELECT
    i.product_id,
    i.store_id,
    i.last_restock,
    ss.units,
    ss.store_id AS store_sale_store_id,
    ss.revenue
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id = 6
ORDER BY i.store_id
