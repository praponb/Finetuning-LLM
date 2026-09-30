SELECT a.id, a.status, a.balance, a.customer_id, c.credit_limit, c.age FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE a.status = 'FROZEN' AND c.name LIKE '%Brown%'
