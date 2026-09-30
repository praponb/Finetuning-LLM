WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id, method), 4) AS amount_share,
        AVG(amount) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
        ROW_NUMBER() OVER w_recent AS seq_no,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id, method), 4) AS amount_share_2,
        AVG(amount) OVER w_grp AS amount_avg_6
    FROM payments
    WHERE order_id IN (24, 34, 36)
    WINDOW w_recent AS (PARTITION BY status ORDER BY paid_at ASC, id ASC), w_grp AS (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount_share,
        amount_avg_2_prior,
        seq_no,
        amount_share_2,
        amount_avg_6,
        ROW_NUMBER() OVER (ORDER BY amount_avg_6 ASC, id ASC) AS event_seq,
        NTILE(10) OVER (ORDER BY amount_share_2 ASC, id ASC) AS amount_share_2_bucket
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount_avg_2_prior,
        amount_share_2,
        amount_avg_6,
        event_seq,
        MAX(seq_no) OVER w_recent AS running_max_seq_no,
        AVG(seq_no) OVER w_recent AS running_avg_seq_no,
        SUM(seq_no) OVER w_recent AS running_sum_seq_no,
        LAG(amount_avg_6, 1) OVER w_hist AS prev_amount_avg_6
    FROM scored
    WINDOW w_recent AS (ORDER BY amount_avg_6 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY amount_share_2_bucket ASC, id ASC)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount_avg_2_prior,
        amount_avg_6,
        event_seq,
        running_max_seq_no,
        running_avg_seq_no,
        running_sum_seq_no,
        prev_amount_avg_6,
        LEAST(event_seq, amount_share_2) AS least_event_seq_amount_share_2
    FROM with_windows
    WHERE running_sum_seq_no BETWEEN 15 AND 35
)
SELECT
    id,
    amount_avg_2_prior,
    amount_avg_6,
    event_seq,
    running_max_seq_no,
    running_sum_seq_no,
    prev_amount_avg_6,
    least_event_seq_amount_share_2
FROM valid;
