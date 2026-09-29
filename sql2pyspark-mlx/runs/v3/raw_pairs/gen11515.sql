SELECT p.amount, p.method, p.order_id, o.shipping_fee FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE o.shipping_fee <= 11 AND p.status = 'SUCCESS'
