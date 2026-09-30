SELECT status, MAX(balance) - MIN(balance) AS balance_range FROM accounts WHERE status IN ('CLOSED', 'FROZEN') GROUP BY status
