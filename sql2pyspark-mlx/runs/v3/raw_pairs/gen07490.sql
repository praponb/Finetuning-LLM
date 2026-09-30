WITH ranked_transactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) row_num
    FROM transactions
)
SELECT
    id,
    merchant,
    txn_time,
    account_id
FROM ranked_transactions
WHERE row_num = 1
ORDER BY id;
