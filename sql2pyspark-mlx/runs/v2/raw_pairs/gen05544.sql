SELECT
    i.id,
    i.quantity,
    ss.sale_date,
    ss.revenue
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY id
