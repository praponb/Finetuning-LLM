SELECT
    i.product_id,
    ss.revenue,
    ss.store_id,
    ss.units
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.quantity <= 230
ORDER BY ss.store_id
