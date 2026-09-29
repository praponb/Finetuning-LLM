SELECT
    i.last_restock,
    i.store_id,
    i.quantity,
    ss.sale_date,
    ss.revenue
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id = 5;
