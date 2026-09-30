SELECT a.id, a.status, a.account_type, c.age FROM accounts a INNER JOIN customers c ON a.customer_id = c.id;
