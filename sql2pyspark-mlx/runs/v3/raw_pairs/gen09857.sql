SELECT
    i.product_id,
    ss.units,
    ss.store_id,
    ss.revenue
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY ss.revenue DESC, ss.store_id;
