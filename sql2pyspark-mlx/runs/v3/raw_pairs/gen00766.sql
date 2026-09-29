WITH ranked AS (
    SELECT
        id,
        status,
        opened_date,
        account_type,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY opened_date) AS rn
    FROM accounts
)
SELECT
    id,
    status,
    opened_date,
    account_type
FROM ranked
WHERE rn = 1;
