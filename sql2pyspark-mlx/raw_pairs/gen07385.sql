SELECT
    status,
    QUARTER(opened_date) AS opened_quarter,
    AVG(balance) AS avg_balance,
    MIN(opened_date) AS first_opened_date
FROM accounts
GROUP BY status, QUARTER(opened_date)
HAVING avg_balance >= 43000
ORDER BY opened_quarter DESC
