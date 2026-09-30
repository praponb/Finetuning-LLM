SELECT t.is_flagged, t.amount, a.balance FROM transactions t JOIN accounts a ON t.account_id = a.id;
