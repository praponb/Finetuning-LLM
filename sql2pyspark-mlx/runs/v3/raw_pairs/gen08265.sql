SELECT status, COUNT(*) AS total_count, MAX(balance) AS max_balance
FROM accounts
GROUP BY status
ORDER BY status
