SELECT t.id, t.amount, a.customer_id FROM transactions t INNER JOIN accounts a ON t.account_id = a.id
