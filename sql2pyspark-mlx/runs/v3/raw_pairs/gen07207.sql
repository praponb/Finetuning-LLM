SELECT p.id, p.order_id, p.amount, o.order_date FROM payments p JOIN orders o ON p.order_id = o.id
