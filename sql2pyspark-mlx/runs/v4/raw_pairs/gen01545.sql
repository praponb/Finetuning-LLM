WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS rnk
    FROM transactions
    WHERE account_id IN (27, 40)
)
SELECT
    id,
    account_id,
    txn_time,
    merchant
FROM numbered
WHERE rnk <= 2
ORDER BY id DESC
