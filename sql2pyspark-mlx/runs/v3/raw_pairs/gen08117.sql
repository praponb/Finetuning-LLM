SELECT p.stock_qty, p.cost, oi.order_id, oi.quantity, oi.product_id FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id;
