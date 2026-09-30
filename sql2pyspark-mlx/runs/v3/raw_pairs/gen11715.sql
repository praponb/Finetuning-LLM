WITH ranked AS (
    SELECT
        id,
        merchant,
        amount,
        txn_type,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS rn
    FROM transactions
)
SELECT
    id,
    merchant,
    amount,
    txn_type
FROM ranked
WHERE rn <= 2;
