SELECT
    i.id,
    i.quantity,
    i.last_restock,
    ss.units,
    ss.product_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue > 3600 AND i.quantity <= 230
