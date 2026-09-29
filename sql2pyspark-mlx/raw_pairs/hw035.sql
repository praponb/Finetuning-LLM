SELECT a.id AS account_id, c.id AS customer_id FROM accounts a FULL OUTER JOIN customers c ON a.customer_id = c.id;
