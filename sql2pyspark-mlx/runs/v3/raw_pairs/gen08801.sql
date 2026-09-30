SELECT oi.id, oi.unit_price, oi.order_id, oi.quantity, p.cost, p.stock_qty FROM order_items oi FULL OUTER JOIN products p ON p.id = oi.product_id WHERE oi.quantity > 7 ORDER BY p.stock_qty DESC;
