SELECT t.account_id, t.merchant, a.customer_id, a.account_type, a.balance FROM transactions t LEFT JOIN accounts a ON a.id = t.account_id ORDER BY merchant
