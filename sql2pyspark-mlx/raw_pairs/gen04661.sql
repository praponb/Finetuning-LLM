WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE txn_type = 'DEBIT'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY txn_time DESC, id) AS row_num
    FROM base
    QUALIFY row_num = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_time,
        is_flagged,
        row_num,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        COUNT(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        MAX(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount
    FROM ranked
    WHERE account_id IN (1, 8, 31, 35)
),
scored_transactions AS (
    -- Running and moving aggregates
    SELECT
        id,
        is_flagged,
        running_avg_amount,
        running_max_amount,
        AVG(running_max_amount) OVER (ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_max_amount,
        SUM(running_max_amount) OVER (ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_max_amount,
        COUNT(running_max_amount) OVER (ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_max_amount,
        MAX(running_max_amount) OVER (ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_max_amount
    FROM scored
)
SELECT
    id,
    running_sum_running_max_amount,
    running_count_running_max_amount
FROM scored_transactions
ORDER BY running_count_running_max_amount NULLS FIRST;
