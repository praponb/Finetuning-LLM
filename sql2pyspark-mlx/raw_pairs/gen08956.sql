SELECT a.balance, a.customer_id, a.status, t.is_flagged, COALESCE(t.amount, 0) AS amount_filled FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id WHERE a.customer_id IS NOT NULL;
