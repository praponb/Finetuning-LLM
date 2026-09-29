SELECT
    status,
    MAX(balance) - MIN(balance) AS balance_range,
    COUNT(balance) AS balance_count,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
FROM accounts
GROUP BY status
