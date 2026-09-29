WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY YEAR(txn_time), account_id ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        NTILE(4) OVER (ORDER BY amount, id) AS amount_bucket
    FROM transactions
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        SUM(amount_bucket) OVER (PARTITION BY account_id ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_bucket,
        ROW_NUMBER() OVER (ORDER BY amount DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS event_seq_2,
        LAG(amount_bucket, 1) OVER w_part AS prev_amount_bucket
    FROM metrics
    WHERE amount_avg_3 > 3928
    WINDOW w_part AS (PARTITION BY account_id ORDER BY amount_avg_3, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        amount_avg_3,
        cumulative_amount,
        event_seq,
        event_seq_2,
        prev_amount_bucket,
        amount_bucket * running_amount_bucket AS amount_bucket_times_running_amount_bucket
    FROM enriched
    WHERE txn_time >= DATE '2023-06-16'
      AND account_id IN (1, 7, 9, 24)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        merchant,
        cumulative_amount,
        event_seq_2,
        amount_bucket_times_running_amount_bucket,
        RANK() OVER (PARTITION BY txn_type, merchant ORDER BY event_seq NULLS LAST) AS event_seq_rank,
        LEAD(amount_avg_3, 1, 0) OVER w_ordered AS next_amount_avg_3,
        COUNT(prev_amount_bucket) OVER w_part AS txn_type_count_prev_amount_bucket
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY merchant, txn_type ORDER BY event_seq_2, id), w_part AS (PARTITION BY txn_type)
)
-- Grouped metrics
SELECT
    merchant,
    PERCENTILE_APPROX(txn_type_count_prev_amount_bucket, ARRAY(0.25, 0.5, 0.75)) AS txn_type_count_prev_amount_bucket_quartiles,
    COUNT(*) FILTER (WHERE merchant = 'Starbucks') AS starbucks_rows
FROM with_windows
GROUP BY merchant
HAVING COUNT(*) FILTER (WHERE merchant = 'Starbucks') >= 1
ORDER BY merchant DESC NULLS LAST
