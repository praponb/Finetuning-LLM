WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(status, 'Unknown') status_filled
    FROM accounts
    WHERE customer_id IS NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.status,
        s.status_filled,
        latest.transaction_txn_type,
        latest.transaction_amount
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type = 'DEBIT'
        )
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.status_filled,
        s.transaction_txn_type,
        x.txn_type
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    transaction_txn_type,
    COUNT(DISTINCT status) AS distinct_status,
    SORT_ARRAY(COLLECT_SET(transaction_txn_type)) AS transaction_txn_type_list,
    COUNT(*) FILTER (WHERE transaction_txn_type = 'FEE') AS fee_rows,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM latest_match
GROUP BY transaction_txn_type
HAVING COUNT(*) FILTER (WHERE transaction_txn_type = 'FEE') >= 1;
