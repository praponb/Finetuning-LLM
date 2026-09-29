SELECT
    i.quantity,
    i.store_id,
    ss.revenue,
    DATE_FORMAT(i.last_restock, 'yyyy-MM-dd') AS last_restock_label
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units BETWEEN 12 AND 24 AND ss.product_id IN (4, 10)
