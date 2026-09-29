SELECT
    i.store_id,
    ss.product_id,
    ss.sale_date,
    ss.revenue
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (1, 5, 8)
ORDER BY product_id DESC, revenue DESC;
