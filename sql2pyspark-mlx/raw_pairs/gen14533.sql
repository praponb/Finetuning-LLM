WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(amount * 0.9, 2) AS adjusted_amount
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Shell'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type, merchant), 4) AS amount_share,
        ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY adjusted_amount DESC, id) AS row_seq,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        LEAD(amount, 1) OVER (ORDER BY adjusted_amount, id) AS next_amount
    FROM valid
    WHERE txn_type IN ('DEBIT', 'CREDIT', 'REFUND')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(amount_avg_3_prior) OVER (PARTITION BY txn_type, merchant ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount_avg_3_prior,
        AVG(amount_avg_3_prior) OVER (PARTITION BY txn_type, merchant ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount_avg_3_prior,
        MAX(amount_avg_3_prior) OVER (PARTITION BY txn_type, merchant ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount_avg_3_prior,
        SUM(amount_avg_3_prior) OVER (PARTITION BY txn_type, merchant ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount_avg_3_prior
    FROM enriched
),
matched_keys AS (
    -- Set difference on keys
    SELECT txn_type, id
    FROM scored
    EXCEPT ALL
    SELECT txn_type, id
    FROM transactions
    WHERE txn_type = 'DEBIT'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.txn_type = k.txn_type AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        txn_time,
        merchant,
        amount_share,
        first_known_amount,
        amount_avg_3_prior,
        next_amount,
        running_avg_amount_avg_3_prior,
        running_max_amount_avg_3_prior,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY txn_type), 4) AS row_seq_share,
        ROUND(next_amount / SUM(next_amount) OVER (PARTITION BY YEAR(txn_time), merchant), 4) AS next_amount_share,
        DENSE_RANK() OVER w_part AS amount_avg_3_prior_dense_rank,
        AVG(running_min_amount_avg_3_prior) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS running_min_amount_avg_3_prior_avg_7d
    FROM verified
    WINDOW w_part AS (PARTITION BY merchant ORDER BY amount_avg_3_prior DESC NULLS FIRST)
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        txn_time,
        first_known_amount,
        amount_avg_3_prior,
        next_amount,
        running_avg_amount_avg_3_prior,
        next_amount_share,
        amount_avg_3_prior_dense_rank,
        CUME_DIST() OVER (ORDER BY running_min_amount_avg_3_prior_avg_7d) AS running_min_amount_avg_3_prior_avg_7d_cume_dist
    FROM with_windows
)
-- Window metrics over several different partitions
SELECT
    first_known_amount,
    next_amount,
    running_avg_amount_avg_3_prior,
    RANK() OVER (ORDER BY first_known_amount) AS first_known_amount_rank,
    PERCENT_RANK() OVER (ORDER BY amount_avg_3_prior DESC) AS amount_avg_3_prior_percent_rank
FROM with_windows_transactions;
