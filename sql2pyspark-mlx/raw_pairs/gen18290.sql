SELECT
    i.id,
    i.last_restock,
    i.quantity,
    ss.product_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue > 3100 AND ss.revenue > 2200
ORDER BY product_id;
