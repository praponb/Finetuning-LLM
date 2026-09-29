SELECT
    i.id,
    i.quantity,
    i.store_id,
    ss.units,
    ss.revenue
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (4, 5, 6);
