WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT account_id, txn_type
    FROM transactions
    INTERSECT
    SELECT account_id, txn_type
    FROM transactions
    WHERE account_id IN (1, 14, 35)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM transactions s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.txn_type = k.txn_type
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    is_flagged
FROM kept_rows
