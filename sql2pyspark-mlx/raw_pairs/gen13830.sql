WITH top_transactions AS (
    SELECT
        id,
        merchant,
        amount,
        txn_type,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS row_num
    FROM transactions
    WHERE TO_DATE(txn_time) < '2023-08-31'
)
SELECT *
FROM top_transactions
WHERE row_num <= 2;
