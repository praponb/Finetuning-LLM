WITH base AS (
    SELECT *
    FROM accounts
    WHERE NOT (customer_id = 34 AND status IS NOT NULL)
),
summary AS (
    SELECT status, COUNT(DISTINCT balance) AS unique_account_types
    FROM base
    GROUP BY status
)
SELECT status, unique_account_types
FROM summary
WHERE unique_account_types > 10
ORDER BY unique_account_types DESC;
