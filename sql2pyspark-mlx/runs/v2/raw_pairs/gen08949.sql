SELECT product_id
FROM order_items
WHERE order_id IN (4, 10, 12, 14)
UNION ALL
SELECT product_id
FROM order_items
WHERE order_id IN (5, 16, 20, 33)
ORDER BY product_id DESC;
