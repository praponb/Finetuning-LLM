SELECT a.id, a.order_id, a.unit_price, a.quantity FROM order_items a WHERE a.unit_price < (SELECT AVG(b.unit_price) FROM order_items b WHERE b.order_id = a.order_id)
