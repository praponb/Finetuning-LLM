SELECT status, account_type, SUM(balance) total_balance
FROM accounts
WHERE opened_date >= '2024-09-03'
GROUP BY status, account_type
ORDER BY status, total_balance DESC
