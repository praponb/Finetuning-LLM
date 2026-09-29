SELECT account_type, COUNT(balance) AS balance_count FROM accounts WHERE (balance <= 64000 AND status <> 'FROZEN') OR status <> 'CLOSED' GROUP BY account_type
