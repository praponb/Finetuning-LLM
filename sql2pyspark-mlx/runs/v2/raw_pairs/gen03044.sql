SELECT t.id, t.amount, t.account_id, a.account_type, a.status, c.age FROM transactions t LEFT OUTER JOIN accounts a ON t.account_id = a.id JOIN customers c ON a.customer_id = c.id;
