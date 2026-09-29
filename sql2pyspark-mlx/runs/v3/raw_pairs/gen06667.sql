SELECT
    i.id,
    i.quantity,
    i.product_id,
    ss.revenue,
    ss.sale_date,
    ss.units
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id = 9 AND ss.revenue < 1500
