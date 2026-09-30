SELECT a.balance, a.customer_id, a.status, t.amount, COALESCE(t.amount, 0) AS amount_filled FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id ORDER BY balance DESC;
