SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    CASE WHEN balance >= 87000 THEN 'high' WHEN balance >= 11000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    account_type,
    SUM(CASE WHEN account_type = 'SAVINGS' THEN balance ELSE 0 END) AS savings_balance,
    MAX(balance) AS max_balance
FROM accounts
WHERE status = 'FROZEN' AND account_type = 'LOAN'
GROUP BY 1, 2, 3
ORDER BY savings_balance;
