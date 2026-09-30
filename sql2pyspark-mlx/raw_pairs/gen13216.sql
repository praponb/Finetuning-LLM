WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        CUME_DIST() OVER w_recent AS balance_cume_dist,
        RANK() OVER w_recent AS balance_rank,
        SUM(balance) OVER (PARTITION BY status ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC NULLS LAST) AS balance_rank_2,
        PERCENT_RANK() OVER w_running AS balance_percent_rank
    FROM accounts
    WHERE YEAR(opened_date) = 2024
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY balance DESC NULLS FIRST), w_running AS (PARTITION BY YEAR(opened_date), status ORDER BY balance DESC NULLS FIRST)
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.balance_cume_dist,
        top1.transaction_merchant
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(balance_cume_dist) OVER w_part AS prev_balance_cume_dist,
        LEAD(balance_cume_dist) OVER w_part AS next_balance_cume_dist,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM latest_match
    WINDOW w_part AS (PARTITION BY transaction_merchant ORDER BY opened_date ASC, id ASC)
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(next_balance_cume_dist) OVER (PARTITION BY transaction_merchant ORDER BY next_balance_cume_dist ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_balance_cume_dist_avg_5,
        NTILE(4) OVER (PARTITION BY YEAR(opened_date) ORDER BY balance_cume_dist DESC, id ASC) AS balance_cume_dist_bucket,
        CUME_DIST() OVER (ORDER BY prev_balance_cume_dist DESC) AS prev_balance_cume_dist_cume_dist,
        AVG(event_seq) OVER (PARTITION BY YEAR(opened_date) ORDER BY next_balance_cume_dist ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_seq_avg_6_prior
    FROM with_windows
),
enriched_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        balance_cume_dist,
        transaction_merchant,
        prev_balance_cume_dist,
        next_balance_cume_dist,
        event_seq,
        balance_cume_dist_bucket,
        prev_balance_cume_dist_cume_dist,
        event_seq_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY transaction_merchant ORDER BY next_balance_cume_dist DESC NULLS LAST) AS next_balance_cume_dist_dense_rank,
        FIRST_VALUE(balance_cume_dist_bucket, TRUE) OVER w_hist AS first_known_balance_cume_dist_bucket,
        FIRST_VALUE(prev_balance_cume_dist) IGNORE NULLS OVER (PARTITION BY transaction_merchant ORDER BY balance_cume_dist_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_balance_cume_dist
    FROM with_windows_accounts
    WINDOW w_hist AS (ORDER BY prev_balance_cume_dist_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.prev_balance_cume_dist,
        s.next_balance_cume_dist,
        s.event_seq,
        s.balance_cume_dist_bucket,
        s.prev_balance_cume_dist_cume_dist,
        s.next_balance_cume_dist_dense_rank,
        top1.transaction_amount,
        top1.txn_time
    FROM enriched_accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows_accounts_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_balance_cume_dist,
        event_seq,
        next_balance_cume_dist_dense_rank,
        txn_time,
        ROW_NUMBER() OVER (ORDER BY prev_balance_cume_dist_cume_dist ASC, id ASC) AS seq_no,
        NTILE(5) OVER w_running AS prev_balance_cume_dist_bucket,
        LAG(next_balance_cume_dist_dense_rank, 1) OVER (ORDER BY prev_balance_cume_dist ASC, id ASC) AS prev_next_balance_cume_dist_dense_rank,
        AVG(balance_cume_dist_bucket) OVER (ORDER BY transaction_amount ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS balance_cume_dist_bucket_avg_5,
        AVG(prev_balance_cume_dist) OVER (ORDER BY next_balance_cume_dist_dense_rank ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS prev_balance_cume_dist_avg_5_prior
    FROM with_latest
    WINDOW w_running AS (ORDER BY prev_balance_cume_dist DESC, id ASC)
)
SELECT
    id,
    prev_balance_cume_dist,
    txn_time,
    prev_balance_cume_dist_bucket,
    prev_next_balance_cume_dist_dense_rank,
    balance_cume_dist_bucket_avg_5
FROM with_windows_accounts_2
ORDER BY prev_balance_cume_dist DESC NULLS LAST
