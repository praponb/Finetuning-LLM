SELECT
    account_type,
    ROUND(STDDEV(balance), 2) AS stddev_balance,
    COUNT(*) AS total_count,
    AVG(balance) AS avg_balance
FROM accounts
GROUP BY account_type
ORDER BY stddev_balance;
