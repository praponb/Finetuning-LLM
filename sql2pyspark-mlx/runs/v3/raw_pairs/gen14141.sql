SELECT
    account_type,
    SUM(balance) AS total_balance,
    MIN(balance) AS min_balance,
    AVG(balance) AS avg_balance
FROM accounts
GROUP BY account_type
ORDER BY 3, account_type
LIMIT 1
