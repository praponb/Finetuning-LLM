SELECT a.id, a.status, c.credit_limit FROM accounts a INNER JOIN customers c ON a.customer_id = c.id;
