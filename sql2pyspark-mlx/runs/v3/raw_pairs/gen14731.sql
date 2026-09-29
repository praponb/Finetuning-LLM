SELECT id, product_id, COUNT(*) OVER (PARTITION BY product_id) AS product_id_count
FROM order_items
WHERE order_id IN (5, 29, 30)
ORDER BY id DESC;
