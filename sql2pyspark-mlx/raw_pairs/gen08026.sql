WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS row_num
        FROM accounts
    ) x
    WHERE row_num = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.opened_date,
        s.row_num,
        latest.txn_time,
        latest.transaction_amount
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance,
        opened_date,
        row_num,
        LEAD(balance) OVER (ORDER BY transaction_amount, id) AS next_balance,
        LAG(transaction_amount) OVER (ORDER BY transaction_amount, id) AS prev_transaction_amount,
        LEAD(transaction_amount) OVER (ORDER BY transaction_amount, id) AS next_transaction_amount,
        ROW_NUMBER() OVER (ORDER BY transaction_amount, id) AS event_seq
    FROM eligible
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.row_num,
        top1.txn_time,
        top1.amount
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.txn_time, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.txn_time
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER w_hist AS amount_bucket,
        NTILE(3) OVER (PARTITION BY TO_DATE(txn_time) ORDER BY balance DESC, id) AS balance_bucket,
        LAG(balance, 1, 0) OVER (ORDER BY txn_time, id) AS prev_balance
    FROM eligible_accounts
    WINDOW w_hist AS (ORDER BY amount, id)
)
-- Ranking and gap features
SELECT
    *,
    MAX(balance) OVER w_running AS running_max_balance,
    MIN(balance) OVER w_running AS running_min_balance,
    COUNT(balance) OVER w_running AS running_count_balance
FROM metrics
WINDOW w_running AS (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
