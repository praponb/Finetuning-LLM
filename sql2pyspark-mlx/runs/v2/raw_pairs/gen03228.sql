WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS rn
    FROM transactions
    WHERE amount > 3000
)
SELECT
    id,
    account_id,
    txn_time,
    is_flagged
FROM Ranked
WHERE rn <= 1
ORDER BY account_id DESC, txn_time
