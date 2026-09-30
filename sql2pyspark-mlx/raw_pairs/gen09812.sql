WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, merchant
    FROM transactions
    EXCEPT DISTINCT
    SELECT id, merchant
    FROM transactions
    WHERE merchant = 'Amazon'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM transactions s
    INNER JOIN matched_keys k ON s.id = k.id AND s.merchant = k.merchant
)
SELECT
    txn_type,
    txn_time,
    merchant
FROM kept_rows
ORDER BY txn_time ASC NULLS LAST
