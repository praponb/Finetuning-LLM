WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY txn_time DESC) AS pos
    FROM transactions
)
SELECT
    id,
    merchant,
    txn_time,
    is_flagged
FROM numbered
WHERE pos = 1;
