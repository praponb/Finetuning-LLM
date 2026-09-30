WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        DATE_FORMAT(txn_time, 'MMM yyyy') AS txn_time_label
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-07-07 17:00:00'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount, 1) OVER w_grp AS next_amount,
        MAX(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d,
        LEAD(amount, 1) OVER w_grp AS next_amount_2
    FROM filtered
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY txn_time, id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY next_amount DESC, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM latest
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount NULLS LAST, id) AS row_num
        FROM sessions_transactions
    ) t
    WHERE row_num = 1
),
ranked_transactions AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC NULLS FIRST, id) AS rn_2
    FROM ranked
    QUALIFY rn_2 = 1
),
scored_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(next_amount, 1) OVER (PARTITION BY merchant ORDER BY session_no, id) AS next_next_amount,
        CUME_DIST() OVER (PARTITION BY merchant ORDER BY amount) AS amount_cume_dist,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS amount_dense_rank
    FROM ranked_transactions
    WHERE new_group_flag > 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        merchant,
        next_amount_2,
        next_amount,
        amount
    FROM scored_transactions
) src
UNPIVOT (
    metric_value FOR kpi IN (next_amount_2, next_amount AS NEXT_AMOUNT, amount AS AMOUNT)
) unp;
