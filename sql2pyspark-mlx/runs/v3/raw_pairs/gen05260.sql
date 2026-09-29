SELECT
    account_type,
    CASE WHEN balance >= 64000 THEN 'high' WHEN balance >= 12000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    MAX(balance) AS max_balance,
    COUNT(balance) AS balance_count,
    MAX(opened_date) AS last_opened_date
FROM accounts
GROUP BY 1, 2
ORDER BY account_type;
