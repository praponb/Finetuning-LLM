SELECT
    i.id,
    i.quantity,
    i.product_id,
    ss.sale_date,
    ss.revenue,
    ss.units
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (2, 10)
