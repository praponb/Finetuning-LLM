WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id) AS rank_in_group
        FROM islands
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY txn_type, account_id ORDER BY amount DESC, id) AS amount_bucket,
        AVG(amount) OVER w_grp AS amount_avg_2_prior
    FROM ranked
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY rank_in_group, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(amount_bucket, 1) OVER (PARTITION BY txn_type ORDER BY island_id, id) AS prev_amount_bucket,
        LEAD(island_id, 1, 0) OVER (ORDER BY amount, id) AS next_island_id,
        DENSE_RANK() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC NULLS LAST) AS amount_dense_rank
    FROM metrics
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT ALL
    SELECT id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 6 AND 10
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    new_group_flag,
    rank_in_group
FROM verified
