SELECT t.id, t.is_flagged, t.merchant, a.status, a.account_type FROM transactions t JOIN accounts a ON t.account_id = a.id ORDER BY a.status DESC;
