SELECT p.id, p.cost, p.launch_date
FROM products p
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.unit_price BETWEEN 170 AND 200)
