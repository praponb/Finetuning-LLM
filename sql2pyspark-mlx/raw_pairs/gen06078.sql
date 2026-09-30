SELECT
    CASE WHEN balance >= 78000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    COUNT_IF(account_type = 'LOAN') AS loan_events,
    ROUND(AVG(balance), 2) AS avg_balance,
    STDDEV_SAMP(balance) AS balance_stddev
FROM accounts
GROUP BY CASE WHEN balance >= 78000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING loan_events > 1;
