SELECT
    account_type,
    MONTH(opened_date) AS opened_month,
    status,
    MIN(opened_date) AS min_opened_date
FROM accounts
GROUP BY 1, 2, 3
ORDER BY min_opened_date, account_type;
