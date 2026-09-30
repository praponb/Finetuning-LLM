SELECT p.id, p.paid_at, o.channel FROM payments p JOIN orders o ON o.id = p.order_id ORDER BY id;
