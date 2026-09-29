SELECT
    i.product_id,
    i.quantity,
    i.last_restock,
    ss.store_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
