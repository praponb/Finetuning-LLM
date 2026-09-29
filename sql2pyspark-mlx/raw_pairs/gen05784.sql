SELECT
    i.id,
    i.last_restock,
    i.product_id,
    ss.units,
    ss.revenue,
    ss.sale_date
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY revenue;
