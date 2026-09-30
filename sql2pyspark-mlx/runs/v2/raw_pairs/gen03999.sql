SELECT
    i.id,
    i.store_id,
    i.last_restock,
    ss.revenue
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue <= 3000 AND i.product_id IN (1, 7)
ORDER BY id DESC;
