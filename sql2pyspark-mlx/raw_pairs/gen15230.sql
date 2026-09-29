WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('/', account_type, status) account_type_status_key,
        COALESCE(status, 'UNKNOWN') status_filled
    FROM accounts
    WHERE opened_date > TO_DATE('2023-06-10')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY opened_date DESC, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type_status_key,
        top1.transaction_txn_type
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.is_flagged
        )
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.txn_type
    FROM kept s
    JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    customer_id,
    account_type_status_key,
    txn_type,
    CASE WHEN txn_type RLIKE '^(FEE|REFUND)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS txn_type_zone
FROM with_latest
ORDER BY id DESC
LIMIT 20;
