WITH src AS (
    SELECT *
    FROM accounts
    WHERE account_type = 'CHECKING' AND opened_date >= '2024-11-01'
),
summary AS (
    SELECT account_type, ROUND(AVG(balance), 2) AS avg_balance
    FROM src
    GROUP BY account_type
)
SELECT account_type, avg_balance
FROM summary
WHERE avg_balance > 76
ORDER BY avg_balance DESC, account_type
LIMIT 5
