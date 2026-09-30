SELECT
    i.id,
    i.product_id,
    i.store_id,
    i.last_restock,
    ss.store_id store_sale_store_id,
    ss.units,
    ss.revenue
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (2, 3, 6)
ORDER BY ss.revenue
