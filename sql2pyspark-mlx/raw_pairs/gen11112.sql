SELECT
    i.id,
    i.last_restock,
    ss.product_id,
    ss.revenue
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE YEAR(i.last_restock) = 2024
