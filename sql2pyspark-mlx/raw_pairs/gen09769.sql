WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE status IS NOT NULL
      AND customer_id IN (2, 15, 25)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        balance,
        balance_int,
        COALESCE(balance, 0) AS balance_filled
    FROM filtered
    WHERE customer_id IS NOT NULL
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance_filled,
        x.transaction_merchant,
        x.transaction_txn_type
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        x.merchant,
        x.transaction_amount
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN transaction_amount >= 4300 THEN 'HIGH' ELSE 'LOW' END AS transaction_amount_tier,
    ROUND(AVG(transaction_amount), 2) AS avg_transaction_amount,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
FROM with_latest
GROUP BY CASE WHEN transaction_amount >= 4300 THEN 'HIGH' ELSE 'LOW' END
HAVING savings_count < 1000
ORDER BY transaction_amount_tier;
