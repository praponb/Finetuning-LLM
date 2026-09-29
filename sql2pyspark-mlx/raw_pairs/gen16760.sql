SELECT
    status,
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    account_type,
    AVG(balance) AS avg_balance,
    MIN(opened_date) AS first_opened_date
FROM accounts
WHERE (balance BETWEEN 33000 AND 58000 AND account_type = 'CHECKING') OR opened_date <= '2025-09-01'
GROUP BY status, opened_month, account_type
