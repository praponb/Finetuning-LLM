SELECT p.amount, p.method, o.channel, o.status, o.discount FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE p.amount > 780 OR p.order_id IS NOT NULL ORDER BY method DESC;
