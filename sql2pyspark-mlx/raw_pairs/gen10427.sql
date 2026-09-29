SELECT o.id, o.status, o.channel, c.name, c.age FROM orders o INNER JOIN customers c ON o.customer_id = c.id ORDER BY o.id;
