SELECT p.id, p.amount, o.discount FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id
