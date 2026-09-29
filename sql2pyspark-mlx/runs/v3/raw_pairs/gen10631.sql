WITH ranked_transactions AS (
    SELECT
        id,
        account_id,
        amount,
        RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) pos
    FROM transactions
)
SELECT id, account_id, amount
FROM ranked_transactions
WHERE pos <= 2
ORDER BY amount
