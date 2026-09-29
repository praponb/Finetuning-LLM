WITH remaining_keys AS (
    -- Set difference on keys
    SELECT account_id, txn_type
    FROM transactions
    EXCEPT ALL
    SELECT account_id, txn_type
    FROM transactions
    WHERE txn_type IN ('FEE', 'CREDIT')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM transactions s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.txn_type = k.txn_type
)
SELECT
    id,
    txn_type,
    amount,
    txn_time,
    merchant,
    CASE WHEN merchant RLIKE '^(Grab|Starbucks)$' THEN 'OTHER_ZONE' WHEN merchant RLIKE '^(7-Eleven|Amazon)$' THEN 'CORE' ELSE 'EXTENDED' END AS merchant_zone
FROM verified
ORDER BY id NULLS FIRST
