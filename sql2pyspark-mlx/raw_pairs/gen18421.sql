WITH top_transactions AS (
    SELECT
        id,
        txn_type,
        txn_time,
        amount,
        RANK() OVER (PARTITION BY txn_type ORDER BY txn_time) AS row_num
    FROM transactions
)
SELECT
    id,
    txn_type,
    txn_time,
    amount
FROM top_transactions
WHERE row_num <= 2;
