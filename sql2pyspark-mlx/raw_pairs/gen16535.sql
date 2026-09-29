WITH with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.amount,
        s.txn_time,
        s.is_flagged,
        p.opened_date
    FROM transactions s
    LEFT JOIN accounts p
        ON s.account_id = p.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        opened_date,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        opened_date,
        SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        session_no,
        DENSE_RANK() OVER (ORDER BY session_no DESC) AS session_no_dense_rank,
        LEAD(session_no, 2, 0) OVER (PARTITION BY YEAR(opened_date), account_id ORDER BY txn_time, id) AS next_session_no,
        NTILE(10) OVER (PARTITION BY account_id ORDER BY session_no, id) AS session_no_bucket,
        COUNT(session_no) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS session_no_count_90d
    FROM sessions_transactions
),
scored_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_time,
        session_no,
        session_no_bucket,
        session_no_count_90d,
        AVG(next_session_no) OVER w_ordered AS next_session_no_avg_2,
        NTILE(10) OVER (PARTITION BY TO_DATE(txn_time), account_id ORDER BY session_no_dense_rank DESC, id) AS session_no_dense_rank_bucket,
        ROUND(session_no_count_90d / SUM(session_no_count_90d) OVER (PARTITION BY account_id), 4) AS session_no_count_90d_share
    FROM scored
    WINDOW w_ordered AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        d.opened_date,
        d.status,
        d.account_type
    FROM scored_transactions a
    JOIN accounts d
        ON a.account_id = d.id
    WHERE d.customer_id IN (3, 5, 16, 37)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims_transactions
),
islands_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Join a dimension and aggregate
SELECT
    p.status AS account_status,
    a.status AS transaction_status,
    COUNT(*) AS num_rows,
    SUM(a.session_no_count_90d_share) AS total_session_no_count_90d_share,
    GROUPING_ID() AS level_id
FROM islands_transactions a
JOIN accounts p ON a.account_id = p.id
GROUP BY GROUPING SETS ((p.status, a.status), (p.status))
