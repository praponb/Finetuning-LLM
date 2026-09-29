WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        TRIM(txn_type) clean_txn_type
    FROM transactions
    WHERE merchant IS NOT NULL
      AND is_flagged
)
-- QUALIFY-style window filter
SELECT
    id,
    account_id,
    txn_time
FROM valid
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC NULLS LAST, id) = 1
