SELECT o.discount, o.amount, p.status, p.amount AS payment_amount FROM orders o LEFT JOIN payments p ON o.id = p.order_id WHERE p.amount > 960;
