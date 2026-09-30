SELECT p.method, p.amount, o.amount AS order_amount, o.discount, o.shipping_fee FROM payments p JOIN orders o ON p.order_id = o.id WHERE o.channel IN ('mobile', 'partner') ORDER BY o.shipping_fee
