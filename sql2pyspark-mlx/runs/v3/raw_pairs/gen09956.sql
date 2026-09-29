SELECT status, last_opened_date, balance_count
FROM (
    SELECT status, MAX(opened_date) AS last_opened_date, COUNT(balance) AS balance_count
    FROM accounts
    GROUP BY status
) AS agg
WHERE balance_count < 8;
