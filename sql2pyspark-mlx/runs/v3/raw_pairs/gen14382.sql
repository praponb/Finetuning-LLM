SELECT
    i.product_id,
    i.store_id,
    i.last_restock,
    ss.units,
    ss.store_id AS store_sale_store_id,
    ss.product_id AS store_sale_product_id
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY ss.store_id;
