SELECT o.id, o.order_id, o.quantity FROM order_items o WHERE o.quantity < (SELECT AVG(o2.quantity) FROM order_items o2 WHERE o2.order_id = o.order_id)
