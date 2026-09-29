SELECT oi.quantity, oi.product_id, oi.order_id, p.product_name, p.cost, p.brand FROM order_items oi JOIN products p ON p.id = oi.product_id WHERE p.stock_qty <= 118 ORDER BY cost ASC;
