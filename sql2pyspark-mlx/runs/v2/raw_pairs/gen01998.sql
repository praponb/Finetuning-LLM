SELECT
    i.id,
    i.last_restock,
    i.quantity,
    i.store_id,
    ss.product_id,
    ss.store_id AS store_sale_store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (2, 3, 10)
ORDER BY i.store_id DESC;
