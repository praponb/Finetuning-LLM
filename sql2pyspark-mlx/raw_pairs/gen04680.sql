WITH joined AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.txn_time,
        a.merchant,
        p.opened_date
    FROM transactions a
    INNER JOIN accounts p
        ON a.account_id = p.id
    WHERE p.balance > 48000
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM joined
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(island_id) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_island_id,
        MAX(island_id) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_island_id,
        AVG(island_id) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_island_id,
        COUNT(island_id) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_island_id
    FROM sessions_transactions
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps_2
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        opened_date,
        island_id,
        running_sum_island_id,
        running_count_island_id,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(island_id) OVER w_running AS last4_avg_island_id,
        SUM(island_id) OVER w_running AS last4_sum_island_id,
        MAX(island_id) OVER w_running AS last4_max_island_id
    FROM islands
    WINDOW w_running AS (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    txn_type,
    metric_name,
    reading
FROM (
    SELECT
        id,
        txn_type,
        amount,
        last4_avg_island_id,
        last4_sum_island_id,
        last4_max_island_id
    FROM with_windows
) src
UNPIVOT (
    reading FOR metric_name IN (amount, last4_avg_island_id, last4_sum_island_id, last4_max_island_id)
);
