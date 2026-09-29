WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE balance > 29000 OR status IN ('FROZEN', 'CLOSED', 'OPEN')
),
summary AS (
    SELECT account_type, ROUND(STDDEV(balance), 2) AS stddev_balance
    FROM filtered_accounts
    GROUP BY account_type
)
SELECT account_type, stddev_balance
FROM summary
WHERE stddev_balance > 45;
