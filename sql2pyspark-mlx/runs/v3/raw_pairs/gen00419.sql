WITH numbered AS (
    SELECT
        id,
        account_id,
        txn_time,
        RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS row_num
    FROM transactions
)
SELECT id, account_id, txn_time
FROM numbered
WHERE row_num = 1
ORDER BY account_id, txn_time
