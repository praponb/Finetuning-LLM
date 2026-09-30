SELECT SUM(balance) AS total_balance, ROUND((100 * SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_frozen
FROM accounts
WHERE status IS NOT NULL AND balance < 55000 AND balance >= 47000
ORDER BY pct_frozen
