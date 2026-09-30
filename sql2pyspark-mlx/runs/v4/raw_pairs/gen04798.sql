SELECT
    i.id,
    i.quantity,
    ss.sale_date,
    ss.product_id
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue < 3700
ORDER BY product_id;
