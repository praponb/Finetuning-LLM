SELECT t.id, t.is_flagged, t.account_id, a.account_type, c.is_active, c.credit_limit FROM transactions t INNER JOIN accounts a ON t.account_id = a.id INNER JOIN customers c ON c.id = a.customer_id;
