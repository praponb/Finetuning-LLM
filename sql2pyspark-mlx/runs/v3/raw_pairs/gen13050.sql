SELECT
    YEAR(opened_date) AS opened_year,
    SUM(CASE WHEN account_type = 'LOAN' THEN balance ELSE 0 END) AS loan_balance,
    SUM(CASE WHEN account_type = 'CREDIT' THEN balance ELSE 0 END) AS credit_balance,
    MAX(opened_date) AS last_opened_date
FROM accounts
GROUP BY YEAR(opened_date)
ORDER BY last_opened_date, loan_balance DESC, opened_year
LIMIT 5
