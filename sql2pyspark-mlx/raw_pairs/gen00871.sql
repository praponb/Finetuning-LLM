SELECT p.id, p.price FROM products p WHERE ARRAY_CONTAINS(p.tags, 'clearance') AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id)
