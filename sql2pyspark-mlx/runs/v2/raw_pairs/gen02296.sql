SELECT
    YEAR(opened_date) AS opened_year,
    status,
    SUM(CASE WHEN account_type = 'LOAN' THEN balance ELSE 0 END) AS loan_balance,
    COUNT(DISTINCT balance) AS unique_statuss
FROM accounts
GROUP BY YEAR(opened_date), status;
