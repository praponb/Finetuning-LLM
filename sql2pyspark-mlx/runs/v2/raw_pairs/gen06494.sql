WITH recent_accounts AS (
    SELECT *, ROUND(balance, 1) AS balance_rounded
    FROM accounts
    WHERE (status = 'CLOSED' AND balance > 23000) OR balance > 34000
),
summary AS (
    SELECT status, account_type, COUNT(*) AS total_count
    FROM recent_accounts
    GROUP BY status, account_type
)
SELECT *
FROM summary
ORDER BY total_count, status DESC
