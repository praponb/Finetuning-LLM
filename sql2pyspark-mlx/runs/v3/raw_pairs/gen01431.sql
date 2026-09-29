SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    account_type,
    customer_id,
    SUM(CASE WHEN account_type = 'SAVINGS' THEN balance ELSE 0 END) AS savings_balance,
    MAX(balance) AS max_balance
FROM accounts
WHERE status = 'FROZEN' AND account_type = 'LOAN'
GROUP BY 1, 2, 3
ORDER BY savings_balance;
