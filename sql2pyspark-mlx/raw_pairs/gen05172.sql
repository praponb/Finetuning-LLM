WITH filtered AS (
    SELECT *, QUARTER(txn_time) AS txn_time_quarter
    FROM transactions
    WHERE merchant IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS rank_in_group
        FROM filtered
    ) ranked
    WHERE rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        rank_in_group,
        AVG(txn_time_quarter) OVER (PARTITION BY merchant ORDER BY txn_time_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_txn_time_quarter,
        MAX(txn_time_quarter) OVER (PARTITION BY merchant ORDER BY txn_time_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_txn_time_quarter,
        SUM(txn_time_quarter) OVER (PARTITION BY merchant ORDER BY txn_time_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_txn_time_quarter,
        COUNT(txn_time_quarter) OVER (PARTITION BY merchant ORDER BY txn_time_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_txn_time_quarter
    FROM latest
),
with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM metrics
),
gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY running_sum_txn_time_quarter DESC, id) AS rn
    FROM sessionized
    QUALIFY rn = 1
),
enriched AS (
    SELECT
        *,
        MAX(running_count_txn_time_quarter) OVER w_grp AS running_max_running_count_txn_time_quarter,
        COUNT(running_count_txn_time_quarter) OVER w_grp AS running_count_running_count_txn_time_quarter,
        SUM(running_count_txn_time_quarter) OVER w_grp AS running_sum_running_count_txn_time_quarter,
        AVG(running_count_txn_time_quarter) OVER w_grp AS running_avg_running_count_txn_time_quarter
    FROM ranked
    WHERE HOUR(txn_time) BETWEEN 8 AND 14
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        rank_in_group,
        running_avg_txn_time_quarter,
        running_max_txn_time_quarter,
        running_sum_txn_time_quarter,
        running_count_txn_time_quarter,
        prev_txn_time,
        is_new_burst,
        session_id,
        rn,
        running_count_running_count_txn_time_quarter,
        running_sum_running_count_txn_time_quarter,
        LAG(running_max_running_count_txn_time_quarter) OVER (PARTITION BY txn_type ORDER BY running_max_running_count_txn_time_quarter, id) AS prev_running_max_running_count_txn_time_quarter,
        LEAD(running_max_running_count_txn_time_quarter) OVER (PARTITION BY txn_type ORDER BY running_max_running_count_txn_time_quarter, id) AS next_running_max_running_count_txn_time_quarter,
        LEAD(running_count_running_count_txn_time_quarter) OVER (PARTITION BY txn_type ORDER BY running_max_running_count_txn_time_quarter, id) AS next_running_count_running_count_txn_time_quarter,
        LAG(running_count_running_count_txn_time_quarter) OVER (PARTITION BY txn_type ORDER BY running_max_running_count_txn_time_quarter, id) AS prev_running_count_running_count_txn_time_quarter,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY running_max_running_count_txn_time_quarter, id) AS seq_no,
        SUM(running_sum_txn_time_quarter) OVER (PARTITION BY merchant, txn_type ORDER BY running_sum_txn_time_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sum_txn_time_quarter
    FROM enriched
)
SELECT
    id,
    txn_type,
    merchant,
    rank_in_group,
    running_avg_txn_time_quarter,
    running_max_txn_time_quarter,
    running_sum_txn_time_quarter,
    running_count_txn_time_quarter,
    prev_txn_time,
    CASE WHEN txn_type RLIKE '^(REFUND|DEBIT)$' THEN 'CORE' ELSE 'GROUP_B' END AS txn_type_zone
FROM with_windows
ORDER BY running_avg_txn_time_quarter NULLS LAST;
