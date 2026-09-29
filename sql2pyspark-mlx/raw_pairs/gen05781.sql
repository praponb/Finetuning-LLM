WITH latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.txn_type
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_txn_type
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.account_type,
        e.balance,
        e.txn_type,
        b.country,
        b.signup_date
    FROM with_latest e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date) ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
        SUM(balance) OVER (PARTITION BY country ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance,
        NTILE(4) OVER (PARTITION BY MONTH(signup_date) ORDER BY balance ASC, id ASC) balance_bucket,
        LAG(balance, 2, 0) OVER (PARTITION BY account_type, country ORDER BY signup_date ASC, id ASC) prev_balance
    FROM with_dims
    WHERE country IN ('DE', 'SG')
),
with_latest_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_amount,
        top1.txn_time
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    account_type,
    country,
    signup_date,
    running_balance,
    cumulative_balance,
    balance_bucket,
    transaction_amount,
    txn_time,
    SUM(transaction_amount) OVER w_grp last7_sum_transaction_amount,
    AVG(transaction_amount) OVER w_grp last7_avg_transaction_amount,
    MAX(transaction_amount) OVER w_grp last7_max_transaction_amount,
    LAST_VALUE(balance_bucket) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY prev_balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_balance_bucket
FROM with_latest_accounts
WINDOW w_grp AS (PARTITION BY txn_type ORDER BY running_balance ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
