SELECT
    id,
    txn_type,
    amount,
    is_flagged
FROM (
    SELECT
        id,
        txn_type,
        amount,
        is_flagged,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) pos
    FROM transactions
) r
WHERE pos = 1;
