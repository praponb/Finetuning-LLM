SELECT o.status, MAX(oi.unit_price) max_unit_price, MAX(oi.quantity) max_quantity FROM order_items oi RIGHT JOIN orders o ON oi.order_id = o.id GROUP BY o.status HAVING MAX(oi.unit_price) > 330
