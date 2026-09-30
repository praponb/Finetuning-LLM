WITH src AS (
    SELECT *
    FROM accounts
    WHERE opened_date > '2025-01-01' AND (customer_id IN (1, 11) OR MONTH(opened_date) IN (1, 3, 10))
),
AccountTypeSummary AS (
    SELECT account_type, status, COUNT(*) AS account_count
    FROM src
    GROUP BY account_type, status
)
SELECT account_type, account_count
FROM AccountTypeSummary
WHERE account_count > 5
ORDER BY account_count DESC, account_type;
