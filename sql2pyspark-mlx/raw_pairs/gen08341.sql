SELECT
    i.last_restock,
    i.store_id,
    ss.product_id,
    ss.units,
    ss.revenue
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE MONTH(i.last_restock) IN (3, 7, 12)
