SELECT p.id, p.method, p.amount, p.order_id, o.status, o.channel FROM payments p INNER JOIN orders o ON o.id = p.order_id ORDER BY p.method DESC;
