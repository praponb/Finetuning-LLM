SELECT
    status,
    CASE WHEN balance >= 81000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(AVG(balance), 2) AS avg_balance_2,
    ROUND(AVG(balance), 2) AS avg_balance_3,
    SUM(CASE WHEN account_type = 'SAVINGS' THEN balance ELSE 0 END) AS savings_balance,
    SUM(balance) FILTER (WHERE status = 'FROZEN') AS frozen_total_balance
FROM accounts
GROUP BY status, CASE WHEN balance >= 81000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', opened_date);
