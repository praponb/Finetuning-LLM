WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(account_type) AS account_type_upper
    FROM accounts
    WHERE opened_date > DATE '2023-06-20'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_balance,
        MAX(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance
    FROM cleaned
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.opened_date,
        s.account_type_upper,
        s.running_max_balance,
        s.running_avg_balance,
        top1.transaction_txn_type,
        top1.merchant
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.account_id IN (5, 18, 29, 35)
        )
)
-- Grouped metrics
SELECT
    transaction_txn_type,
    SUM(CASE WHEN transaction_txn_type = 'REFUND' THEN balance ELSE 0 END) AS refund_balance,
    COUNT(DISTINCT merchant) AS distinct_merchants,
    SUM(CASE WHEN transaction_txn_type = 'CREDIT' THEN running_avg_balance ELSE 0 END) AS credit_running_avg_balance
FROM kept
GROUP BY transaction_txn_type
ORDER BY distinct_merchants;
