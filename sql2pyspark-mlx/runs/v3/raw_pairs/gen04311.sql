SELECT
    i.last_restock,
    ss.store_id,
    ss.sale_date,
    ss.units
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue < 2500 AND i.quantity < 147
