SELECT
    i.id,
    i.product_id,
    i.store_id,
    i.quantity,
    ss.units,
    ss.product_id AS store_sale_product_id,
    ss.revenue
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY product_id
