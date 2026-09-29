SELECT t.id, t.amount, t.account_id, a.customer_id FROM transactions t INNER JOIN accounts a ON a.id = t.account_id WHERE t.is_flagged = TRUE ORDER BY id
