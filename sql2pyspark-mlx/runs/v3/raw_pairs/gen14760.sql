SELECT a.status, a.account_type, a.balance, t.account_id FROM accounts a LEFT OUTER JOIN transactions t ON t.account_id = a.id ORDER BY balance DESC
