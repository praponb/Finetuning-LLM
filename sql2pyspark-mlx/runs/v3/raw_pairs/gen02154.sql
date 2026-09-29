SELECT oi.id, oi.product_id FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE launch_date >= '2025-03-30') ORDER BY oi.id DESC, oi.product_id;
