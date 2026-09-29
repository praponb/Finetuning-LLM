SELECT
    i.id,
    i.store_id,
    i.last_restock,
    i.quantity,
    ss.sale_date,
    ss.units,
    ss.product_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date < '2025-05-18'
