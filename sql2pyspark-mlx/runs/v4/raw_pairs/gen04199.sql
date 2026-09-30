SELECT status, total_balance, balance_range
FROM (
    SELECT status, SUM(balance) AS total_balance, MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    GROUP BY status
) AS sub
WHERE balance_range < 60;
