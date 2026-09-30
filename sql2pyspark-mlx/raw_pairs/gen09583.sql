SELECT opened_quarter, num_accounts
FROM (
    SELECT QUARTER(opened_date) AS opened_quarter, COUNT(*) AS num_accounts
    FROM accounts
    GROUP BY QUARTER(opened_date)
) AS t
WHERE num_accounts >= 8
