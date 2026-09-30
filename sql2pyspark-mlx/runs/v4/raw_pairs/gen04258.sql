SELECT a.customer_id, c.name FROM accounts a INNER JOIN customers c ON c.id = a.customer_id ORDER BY a.customer_id;
