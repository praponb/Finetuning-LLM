SELECT o.discount, p.order_id, p.method, ROUND(p.amount, 1) AS amount_rounded FROM orders o LEFT JOIN payments p ON p.order_id = o.id WHERE p.amount <= 450 OR o.status = 'SHIPPED';
