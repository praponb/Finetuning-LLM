SELECT o.id, o.order_id, o.unit_price FROM order_items o WHERE o.unit_price > (SELECT AVG(o2.unit_price) FROM order_items o2 WHERE o2.order_id = o.order_id);
