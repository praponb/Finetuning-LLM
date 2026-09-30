WITH base AS (
    SELECT *, LENGTH(account_type) account_type_length
    FROM accounts
    WHERE opened_date > DATE '2024-04-21'
      AND customer_id IS NOT NULL
)
SELECT account_type
FROM base
EXCEPT DISTINCT
SELECT account_type
FROM accounts
WHERE opened_date >= '2024-07-01';
