WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(account_type, 1, 4) AS account_type_prefix
    FROM accounts
    WHERE opened_date >= DATE '2023-07-18'
      AND customer_id IS NOT NULL
)
SELECT account_type
FROM valid
INTERSECT
SELECT account_type
FROM accounts
WHERE status <> 'FROZEN'
