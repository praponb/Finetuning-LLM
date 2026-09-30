SELECT p.status, p.amount, o.channel, o.amount AS order_amount, o.store_id FROM payments p JOIN orders o ON p.order_id = o.id;
