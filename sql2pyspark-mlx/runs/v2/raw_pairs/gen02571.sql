SELECT oi.id, oi.order_id, oi.quantity, oi.unit_price, p.cost FROM order_items oi LEFT OUTER JOIN products p ON p.id = oi.product_id ORDER BY oi.unit_price
