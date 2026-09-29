SELECT
    status,
    account_type,
    MAX(opened_date) AS latest_opened_date,
    SUM(balance) AS total_balance,
    AVG(balance) AS avg_balance
FROM accounts
GROUP BY status, account_type
ORDER BY total_balance, account_type DESC
