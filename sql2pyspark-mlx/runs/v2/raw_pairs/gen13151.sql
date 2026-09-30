SELECT
    i.id,
    i.product_id,
    i.store_id,
    i.last_restock,
    ss.store_id AS store_sale_store_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY store_sale_store_id DESC;
