SELECT oi.id, oi.product_id, oi.order_id, oi.quantity, p.stock_qty, p.cost, ROUND(oi.quantity / oi.unit_price, 2) line_total FROM order_items oi JOIN products p ON oi.product_id = p.id
