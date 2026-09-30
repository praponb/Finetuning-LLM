WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        DATEDIFF(CURRENT_DATE(), txn_time) / 7 weeks_since_txn_time,
        ROUND(amount, 1) amount_rounded
    FROM transactions
    WHERE merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount_rounded) OVER (PARTITION BY txn_type, merchant ORDER BY weeks_since_txn_time ASC, id ASC) next_amount_rounded,
        LAG(weeks_since_txn_time) OVER (PARTITION BY txn_type, merchant ORDER BY weeks_since_txn_time ASC, id ASC) prev_weeks_since_txn_time,
        ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY weeks_since_txn_time ASC, id ASC) row_seq
    FROM islands
    WHERE is_flagged
),
scored_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(amount_rounded) OVER w_ordered running_min_amount_rounded,
        AVG(amount_rounded) OVER w_ordered running_avg_amount_rounded,
        SUM(amount_rounded) OVER w_ordered running_sum_amount_rounded,
        COUNT(amount_rounded) OVER w_ordered running_count_amount_rounded,
        SUM(row_seq) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_row_seq
    FROM scored
    WINDOW w_ordered AS (PARTITION BY txn_type ORDER BY prev_weeks_since_txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Running and moving aggregates
SELECT
    id,
    amount,
    txn_time,
    burst_id,
    prev_weeks_since_txn_time,
    running_min_amount_rounded,
    running_avg_amount_rounded,
    running_count_amount_rounded,
    AVG(burst_id) OVER (PARTITION BY MONTH(txn_time), txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) burst_id_avg_2_prior,
    AVG(running_avg_amount_rounded) OVER (PARTITION BY merchant ORDER BY burst_id ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) running_avg_amount_rounded_avg_2,
    SUM(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_sum_7d,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY weeks_since_txn_time ASC, id ASC) row_seq_2
FROM scored_transactions
