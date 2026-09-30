SELECT account_type, CASE WHEN status IN ('FROZEN', 'CLOSED') THEN 'core' ELSE 'other' END AS status_group, ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
WHERE balance >= 56000 OR balance BETWEEN 22000 AND 59000
GROUP BY account_type, CASE WHEN status IN ('FROZEN', 'CLOSED') THEN 'core' ELSE 'other' END
ORDER BY account_type DESC, status_group
LIMIT 1
