WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        COALESCE(status, 'Unknown') AS status_filled
    FROM accounts
    WHERE status IS NOT NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.status_filled,
        latest.transaction_merchant,
        latest.transaction_txn_type
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM with_last
    INTERSECT
    SELECT customer_id
    FROM accounts
    WHERE customer_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type != 'REFUND'
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance >= 70000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    transaction_txn_type,
    MAX(balance) AS max_balance,
    APPROX_PERCENTILE(balance, 0.95) AS balance_p95,
    COUNT(CASE WHEN transaction_merchant = 'Lazada' THEN 1 END) AS lazada_count,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM not_flagged
GROUP BY CASE WHEN balance >= 70000 THEN 'HIGH' ELSE 'LOW' END, transaction_txn_type
