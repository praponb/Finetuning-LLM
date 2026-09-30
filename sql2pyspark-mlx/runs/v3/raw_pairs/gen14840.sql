SELECT
    i.id,
    i.last_restock,
    i.store_id,
    ss.revenue,
    ss.sale_date
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue < 1200
ORDER BY i.id DESC
