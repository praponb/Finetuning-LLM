SELECT
    i.id,
    i.store_id,
    i.last_restock,
    ss.product_id,
    ss.store_id AS store_sale_store_id,
    ss.revenue,
    DATE_TRUNC('year', i.last_restock) AS last_restock_period
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date < '2025-06-11' AND i.quantity != 87;
