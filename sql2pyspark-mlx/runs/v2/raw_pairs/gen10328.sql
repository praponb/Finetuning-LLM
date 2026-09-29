SELECT p.id, p.order_id, p.amount, o.store_id, o.channel FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE o.discount <= 0.28
