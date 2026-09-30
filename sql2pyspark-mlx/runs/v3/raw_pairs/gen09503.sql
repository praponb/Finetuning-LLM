SELECT
    i.store_id,
    ss.revenue,
    ss.store_id AS store_sale_store_id,
    ss.units
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock >= '2024-12-01' AND ss.store_id = 2
ORDER BY i.store_id DESC;
