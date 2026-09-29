SELECT
    i.id,
    i.store_id,
    ss.sale_date,
    ss.store_id AS store_sale_store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (3, 6, 7)
