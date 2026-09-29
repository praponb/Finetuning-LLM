select id, amount, order_id, paid_at from payments where not (method = 'cash' and method not in ('card', 'promptpay', 'cash'));
