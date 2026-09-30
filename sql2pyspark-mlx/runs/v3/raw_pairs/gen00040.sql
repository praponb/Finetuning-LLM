SELECT
    i.id,
    i.store_id,
    ss.product_id,
    ss.sale_date
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (2, 8, 9) AND ss.revenue < 3600;
