SELECT p.method, p.amount, o.customer_id, o.status, o.channel FROM payments p JOIN orders o ON p.order_id = o.id WHERE o.channel IN ('web', 'store', 'mobile')
