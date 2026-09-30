SELECT a.id, a.account_type, a.balance, a.status FROM accounts a WHERE a.balance = (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.account_type = a.account_type) ORDER BY a.balance DESC
