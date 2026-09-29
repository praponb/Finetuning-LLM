WITH top_accounts AS (
    SELECT
        id,
        status,
        balance,
        customer_id,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY balance) AS rnk
    FROM accounts
)
SELECT
    id,
    status,
    balance,
    customer_id
FROM top_accounts
WHERE rnk <= 5
