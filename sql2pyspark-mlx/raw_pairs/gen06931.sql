WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        opened_date,
        CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE opened_date > DATE '2023-06-17'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        opened_date,
        balance_int
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC NULLS FIRST) = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.opened_date,
        s.balance_int,
        top1.transaction_amount,
        top1.txn_type
    FROM deduped s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant = 'Starbucks'
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY opened_date DESC NULLS FIRST, id) AS row_num
    FROM eligible
    QUALIFY row_num = 1
)
-- Grouped metrics
SELECT
    txn_type,
    COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count,
    COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count,
    AVG(transaction_amount) FILTER (WHERE txn_type = 'CREDIT') AS credit_avg_transaction_amount
FROM latest
GROUP BY txn_type
ORDER BY credit_count
