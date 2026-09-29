SELECT *
FROM (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, COUNT(DISTINCT balance) AS unique_account_types
    FROM accounts
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
) AS sub
WHERE unique_account_types >= 9
ORDER BY unique_account_types DESC
