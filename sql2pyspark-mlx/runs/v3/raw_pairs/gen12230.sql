SELECT t.txn_type, t.is_flagged, a.balance, c.is_active, c.country FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id INNER JOIN customers c ON c.id = a.customer_id
