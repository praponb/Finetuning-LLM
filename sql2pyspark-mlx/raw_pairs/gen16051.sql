SELECT p.id, p.launch_date, p.price
FROM products p
WHERE p.launch_date < '2024-05-01' AND p.id IN (SELECT product_id FROM order_items);
