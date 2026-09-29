SELECT
    i.quantity,
    i.store_id,
    i.last_restock,
    ss.store_id AS store_sale_store_id,
    ss.revenue,
    DATE_SUB(i.last_restock, 32) AS last_restock_minus_32d
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (1, 5);
