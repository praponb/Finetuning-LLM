SELECT o.id, o.status, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE c.city IN ('Singapore', 'New York');
