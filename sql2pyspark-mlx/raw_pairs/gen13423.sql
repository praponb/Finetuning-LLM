SELECT e.id, e.order_id, e.quantity, e.unit_price, b.amount, b.discount, b.status FROM order_items e LEFT JOIN orders b ON e.order_id = b.id;
