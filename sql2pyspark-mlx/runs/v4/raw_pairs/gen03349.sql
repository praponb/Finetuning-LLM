WITH ranked AS (
    SELECT
        id,
        status,
        balance,
        account_type,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) rn
    FROM accounts
)
SELECT
    id,
    status,
    balance,
    account_type
FROM ranked
WHERE rn <= 3
