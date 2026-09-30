SELECT
    i.product_id,
    i.quantity,
    ss.revenue,
    ss.store_id
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue IS NOT NULL AND ss.revenue >= 2000;
