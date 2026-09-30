SELECT a.opened_date, a.balance, a.customer_id, c.credit_limit, c.email FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE c.age >= 57 AND a.balance >= 50000;
