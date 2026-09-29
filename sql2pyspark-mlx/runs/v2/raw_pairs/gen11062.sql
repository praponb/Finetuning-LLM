SELECT
    i.quantity,
    ss.product_id,
    ss.revenue,
    FLOOR(ss.units) AS units_whole
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
