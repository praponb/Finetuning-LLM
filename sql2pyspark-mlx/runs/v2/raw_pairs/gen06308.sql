WITH src AS (
    SELECT *
    FROM accounts
    WHERE account_type = 'LOAN' AND balance BETWEEN 37000 AND 48000
),
status_summary AS (
    SELECT status, MIN(balance) AS min_balance
    FROM src
    GROUP BY status
)
SELECT status, min_balance
FROM status_summary
WHERE min_balance > 53000
ORDER BY min_balance DESC, status
LIMIT 1;
