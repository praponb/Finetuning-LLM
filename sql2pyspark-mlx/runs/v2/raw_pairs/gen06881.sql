SELECT p.id, p.status, o.amount FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE o.shipping_fee < 21
