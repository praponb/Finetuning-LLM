WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant, account_id ORDER BY amount DESC, id) = 1
)
SELECT
    id,
    account_id,
    txn_type,
    is_flagged
FROM deduped;
