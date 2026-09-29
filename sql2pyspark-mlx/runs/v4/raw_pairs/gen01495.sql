SELECT p.paid_at, o.channel, c.segment FROM payments p LEFT OUTER JOIN orders o ON o.id = p.order_id INNER JOIN customers c ON c.id = o.customer_id;
