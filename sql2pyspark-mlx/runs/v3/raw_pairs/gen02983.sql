SELECT account_type, balance_count, min_opened_date
FROM (
    SELECT account_type, COUNT(balance) AS balance_count, MIN(opened_date) AS min_opened_date
    FROM accounts
    GROUP BY account_type
) AS s
WHERE balance_count >= 8
