WITH ranked_accounts AS (
    SELECT
        id,
        customer_id,
        balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rn
    FROM accounts
)
SELECT id, customer_id, balance
FROM ranked_accounts
WHERE rn <= 3;
