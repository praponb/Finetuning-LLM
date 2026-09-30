SELECT a.account_type, a.status, t.is_flagged, t.amount, t.txn_type FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id ORDER BY amount;
