SELECT
    status,
    account_type,
    MAX(balance) - MIN(balance) AS balance_range,
    SUM(balance) AS total_balance
FROM accounts
GROUP BY status, account_type
HAVING total_balance < 399000
ORDER BY account_type
