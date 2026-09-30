WITH ranked_transactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS row_num
    FROM transactions
    WHERE account_id IN (9, 14)
)
SELECT
    id,
    txn_type,
    amount,
    is_flagged
FROM ranked_transactions
WHERE row_num <= 3
