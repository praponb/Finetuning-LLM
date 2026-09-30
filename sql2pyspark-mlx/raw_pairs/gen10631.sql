WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        CASE WHEN account_type IN ('LOAN', 'SAVINGS') THEN 'Core' ELSE 'Other' END AS account_type_group
    FROM accounts
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        LAG(balance, 2) OVER (PARTITION BY DATE_TRUNC('month', opened_date), account_type ORDER BY opened_date, id) AS prev_balance
    FROM base
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.opened_date,
        s.account_type_group,
        s.running_balance,
        top1.transaction_txn_time,
        top1.transaction_merchant
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        balance,
        opened_date,
        account_type_group,
        running_balance,
        transaction_txn_time
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_merchant ORDER BY transaction_txn_time NULLS FIRST, id) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        balance,
        opened_date,
        transaction_txn_time,
        HOUR(transaction_txn_time) AS transaction_txn_time_hour,
        FLOOR(balance) AS balance_whole
    FROM ranked
    WHERE opened_date > DATE '2023-06-28'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN transaction_txn_time_hour >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS transaction_txn_time_hour_tier,
    MIN(balance_whole) AS min_balance_whole,
    MIN(transaction_txn_time) AS started_at,
    (UNIX_TIMESTAMP(MAX(transaction_txn_time)) - UNIX_TIMESTAMP(MIN(transaction_txn_time))) / 60 AS duration_min
FROM cleaned
GROUP BY 1
HAVING min_balance_whole < 1000000
ORDER BY duration_min DESC;
