SELECT opened_quarter, balance_count
FROM (
    SELECT QUARTER(opened_date) AS opened_quarter, COUNT(balance) AS balance_count
    FROM accounts
    GROUP BY QUARTER(opened_date)
) AS t
WHERE balance_count < 8;
