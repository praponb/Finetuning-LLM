WITH src AS (
    SELECT *
    FROM accounts
    WHERE account_type IN ('LOAN', 'SAVINGS') AND (opened_date >= '2025-09-26' OR balance BETWEEN 44000 AND 56000)
),
summary AS (
    SELECT status, COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count
    FROM src
    GROUP BY status
)
SELECT *
FROM summary
ORDER BY loan_count DESC, status ASC
LIMIT 50;
