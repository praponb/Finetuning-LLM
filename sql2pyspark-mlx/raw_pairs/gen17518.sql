SELECT
    status,
    account_type,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT(*) AS cnt
FROM accounts
GROUP BY status, account_type;
