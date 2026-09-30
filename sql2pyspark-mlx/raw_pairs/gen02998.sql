WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        status,
        INITCAP(TRIM(status)) AS status_title,
        SUBSTRING(status, 1, 2) AS status_prefix,
        COALESCE(method, 'NONE') AS method_filled
    FROM payments
    WHERE paid_at >= DATE '2023-07-09'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY amount, id) AS seq_no,
        LAG(amount, 1) OVER (PARTITION BY status ORDER BY amount, id) AS prev_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share
    FROM filtered
),
enriched_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY prev_amount DESC NULLS FIRST) AS prev_amount_dense_rank,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY status), 4) AS seq_no_share,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY seq_no, id) AS event_seq
    FROM enriched
    WHERE status = 'FAILED'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        status_title,
        status_prefix,
        prev_amount,
        prev_amount_dense_rank,
        event_seq,
        ROW_NUMBER() OVER w_grp AS seq_no_2,
        MAX(seq_no) OVER (PARTITION BY status) AS status_max_seq_no,
        AVG(prev_amount_dense_rank) OVER (PARTITION BY status ORDER BY prev_amount_dense_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_amount_dense_rank_avg_3_prior
    FROM enriched_payments
    WINDOW w_grp AS (PARTITION BY status ORDER BY amount, id)
),
filtered_payments AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(prev_amount, amount) AS least_prev_amount_amount
    FROM with_windows
    WHERE prev_amount_dense_rank < 6
)
-- Running and moving aggregates
SELECT
    *,
    MAX(prev_amount_dense_rank_avg_3_prior) OVER w_grp AS last4_max_prev_amount_dense_rank_avg_3_prior,
    SUM(prev_amount_dense_rank_avg_3_prior) OVER w_grp AS last4_sum_prev_amount_dense_rank_avg_3_prior,
    AVG(prev_amount_dense_rank_avg_3_prior) OVER w_grp AS last4_avg_prev_amount_dense_rank_avg_3_prior,
    COUNT(prev_amount_dense_rank_avg_3_prior) OVER w_grp AS last4_count_prev_amount_dense_rank_avg_3_prior
FROM filtered_payments
WINDOW w_grp AS (ORDER BY seq_no_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
