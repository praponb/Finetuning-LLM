WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(txn_time) AS txn_time_date,
        DAYOFWEEK(txn_time) AS txn_time_dayofweek
    FROM transactions
    WHERE txn_time >= TO_DATE('2023-06-24')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time_dayofweek DESC NULLS FIRST, id) = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_time_date,
        txn_time_dayofweek,
        new_group_flag,
        session_no,
        SUM(session_no) OVER w_running AS cumulative_session_no,
        MAX(txn_time_dayofweek) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS txn_time_dayofweek_max_90d,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time_date, id) AS event_seq,
        LAST_VALUE(merchant) IGNORE NULLS OVER (ORDER BY new_group_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM sessionized
    WINDOW w_running AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM metrics
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', txn_time_date) AS year_start,
        MAX(txn_time_dayofweek_max_90d) FILTER (WHERE txn_type = 'REFUND') AS refund_max_txn_time_dayofweek_max_90d,
        MAX(prev_txn_time) AS last_seen,
        SUM(CASE WHEN txn_type = 'DEBIT' THEN new_group_flag ELSE 0 END) AS debit_new_group_flag,
        AVG(first_known_amount) AS avg_first_known_amount,
        SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
        AVG(is_new_session) FILTER (WHERE merchant = 'Amazon') AS amazon_avg_is_new_session
    FROM sessions_transactions
    GROUP BY year_start
)
SELECT
    year_start,
    refund_max_txn_time_dayofweek_max_90d,
    last_seen,
    debit_new_group_flag,
    avg_first_known_amount
FROM totals
ORDER BY debit_new_group_flag NULLS LAST
