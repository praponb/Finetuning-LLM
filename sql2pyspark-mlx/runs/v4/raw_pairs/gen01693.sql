SELECT a.id, a.order_id, a.quantity, a.product_id FROM order_items a WHERE a.quantity > (SELECT AVG(b.quantity) FROM order_items b WHERE b.order_id = a.order_id)
