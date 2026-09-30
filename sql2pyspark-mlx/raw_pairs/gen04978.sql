SELECT a.id, a.order_id, a.quantity FROM order_items a WHERE a.quantity = (SELECT MIN(b.quantity) FROM order_items b WHERE b.order_id = a.order_id) ORDER BY a.id DESC, a.quantity DESC;
