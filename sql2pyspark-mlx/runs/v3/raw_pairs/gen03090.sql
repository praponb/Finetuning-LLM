WITH ranked AS (
    SELECT
        id,
        status,
        balance,
        customer_id,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS rn
    FROM accounts
)
SELECT
    id,
    status,
    balance,
    customer_id
FROM ranked
WHERE rn = 1
ORDER BY id ASC, status ASC;
