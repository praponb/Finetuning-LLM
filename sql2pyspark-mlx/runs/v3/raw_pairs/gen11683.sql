SELECT
    ss.product_id,
    ss.store_id,
    MAX(i.quantity) - MIN(i.quantity) quantity_range,
    MIN(i.quantity) min_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.quantity < 226
GROUP BY ss.product_id, ss.store_id
ORDER BY store_id DESC;
