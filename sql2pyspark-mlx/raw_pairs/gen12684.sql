SELECT
    i.id,
    i.quantity,
    i.product_id,
    i.last_restock,
    ss.sale_date,
    ss.store_id,
    ss.units
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (4, 5) AND ss.revenue < 3300;
