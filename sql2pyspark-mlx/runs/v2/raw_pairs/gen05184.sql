WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS rnk
    FROM transactions
)
SELECT
    id,
    merchant,
    txn_time,
    account_id
FROM numbered
WHERE rnk <= 3;
