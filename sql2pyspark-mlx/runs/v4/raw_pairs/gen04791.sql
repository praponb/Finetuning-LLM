WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT account_id, id
    FROM transactions
    INTERSECT
    SELECT account_id, id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 8 AND 15
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM transactions s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.id = k.id
)
SELECT
    id,
    account_id,
    amount,
    txn_time,
    is_flagged
FROM reconciled;
