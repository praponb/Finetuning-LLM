SELECT status, max_balance, balance_range
FROM (
    SELECT status, MAX(balance) AS max_balance, MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    WHERE YEAR(opened_date) = 2024
    GROUP BY status
) AS x
WHERE max_balance > 67000;
