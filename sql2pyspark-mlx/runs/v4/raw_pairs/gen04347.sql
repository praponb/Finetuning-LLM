SELECT
    i.id,
    i.product_id,
    ss.revenue,
    ss.product_id AS store_sale_product_id,
    ss.store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue BETWEEN 1700 AND 2500
ORDER BY product_id DESC, store_id
