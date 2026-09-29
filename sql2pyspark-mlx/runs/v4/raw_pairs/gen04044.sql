WITH kept_keys AS (
    SELECT merchant, id
    FROM transactions
    EXCEPT ALL
    SELECT merchant, id
    FROM transactions
    WHERE account_id IN (5, 8, 10, 15)
),
reconciled AS (
    SELECT s.*
    FROM transactions s
    JOIN kept_keys k ON s.merchant = k.merchant AND s.id = k.id
)
SELECT
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant
FROM reconciled;
