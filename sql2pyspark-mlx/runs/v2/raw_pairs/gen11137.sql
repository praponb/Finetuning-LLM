SELECT
    i.id,
    i.last_restock,
    i.store_id,
    i.quantity,
    ss.revenue,
    ss.product_id,
    ss.store_id AS store_sale_store_id,
    CEIL(i.quantity) AS quantity_whole
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
