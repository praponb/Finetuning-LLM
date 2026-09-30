SELECT p.launch_date, oi.product_id, oi.order_id
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
WHERE p.stock_qty BETWEEN 152 AND 379 AND p.product_name LIKE '%Hub%'
ORDER BY 2 DESC, oi.order_id
