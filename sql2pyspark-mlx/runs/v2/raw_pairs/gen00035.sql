SELECT product_id, COUNT(*) AS total_count, COUNT(DISTINCT quantity) AS unique_products
FROM order_items
WHERE order_id IN (1, 21, 30, 36)
GROUP BY product_id;
