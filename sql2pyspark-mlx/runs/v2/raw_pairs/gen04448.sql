SELECT
    account_type,
    MAX(balance) AS max_balance,
    AVG(balance) AS avg_balance,
    COUNT(*) AS num_accounts
FROM accounts
GROUP BY account_type
HAVING COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) > 1
