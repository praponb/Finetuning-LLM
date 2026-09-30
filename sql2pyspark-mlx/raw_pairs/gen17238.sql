WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        LOWER(account_type) AS account_type_lower
    FROM accounts
    WHERE customer_id IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC NULLS FIRST, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance,
        status,
        LEAD(balance, 1) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_balance,
        MAX(balance) OVER (PARTITION BY account_type) AS account_type_max_balance,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        DENSE_RANK() OVER (ORDER BY balance DESC) AS balance_dense_rank
    FROM ranked
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS seq_no,
        STDDEV_SAMP(account_type_max_balance) OVER (PARTITION BY status) AS status_stddev_samp_account_type_max_balance
    FROM scored
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_time,
        top1.amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(seq_no) OVER w_ordered AS last4_min_seq_no,
        AVG(seq_no) OVER w_ordered AS last4_avg_seq_no,
        MAX(seq_no) OVER w_ordered AS last4_max_seq_no,
        SUM(seq_no) OVER w_ordered AS last4_sum_seq_no
    FROM with_latest
    WINDOW w_ordered AS (PARTITION BY status ORDER BY status_stddev_samp_account_type_max_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        next_balance,
        status_stddev_samp_account_type_max_balance,
        transaction_txn_time,
        amount,
        last4_avg_seq_no,
        last4_max_seq_no,
        LEAD(next_balance, 1) OVER (PARTITION BY DATE_TRUNC('MONTH', transaction_txn_time), status ORDER BY last4_sum_seq_no, id) AS next_next_balance,
        NTILE(3) OVER (ORDER BY balance, id) AS balance_bucket,
        LAG(account_type_max_balance, 1) OVER (ORDER BY amount, id) AS prev_account_type_max_balance
    FROM with_windows
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', transaction_txn_time) AS year_start,
        (UNIX_TIMESTAMP(MAX(transaction_txn_time)) - UNIX_TIMESTAMP(MIN(transaction_txn_time))) / 60 AS duration_min,
        COUNT(DISTINCT status) AS distinct_status,
        ROUND(AVG(amount), 2) AS avg_amount
    FROM enriched_accounts
    GROUP BY DATE_TRUNC('YEAR', transaction_txn_time)
    HAVING COUNT(DISTINCT status) < 1000
)
SELECT year_start, duration_min
FROM grouped
ORDER BY year_start DESC NULLS FIRST;
