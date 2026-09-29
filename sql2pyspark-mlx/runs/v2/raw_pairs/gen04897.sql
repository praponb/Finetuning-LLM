SELECT a.id, a.status, a.account_type, c.email, c.segment FROM accounts a INNER JOIN customers c ON a.customer_id = c.id ORDER BY a.account_type, id LIMIT 50;
