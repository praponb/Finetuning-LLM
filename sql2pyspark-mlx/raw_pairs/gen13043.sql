SELECT *
FROM (
    SELECT status, MIN(balance) AS min_balance
    FROM accounts
    GROUP BY status
) AS s
WHERE min_balance >= 28000
