WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        session_start_flag,
        session_no,
        CEIL(session_no) AS session_no_whole
    FROM islands
    WHERE session_no BETWEEN 2 AND 3
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY session_start_flag, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT txn_type
    FROM deduped
    INTERSECT
    SELECT txn_type
    FROM transactions
    WHERE is_flagged
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.txn_type = k.txn_type
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        session_start_flag,
        session_no,
        session_no_whole,
        dedup_rank,
        session_no + session_start_flag AS session_no_plus_session_start_flag
    FROM verified
    WHERE txn_time >= DATE '2023-05-30'
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        d.balance,
        d.account_type
    FROM cleaned s
    JOIN accounts d
        ON s.account_id = d.id
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT s.*, b.account_type AS account_account_type
    FROM enriched s
    LEFT JOIN accounts b
        ON s.account_id = b.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        account_account_type,
        COUNT(*) AS num_rows,
        SIZE(COLLECT_SET(account_type)) AS num_account_types
    FROM enriched_transactions
    GROUP BY account_account_type
    HAVING num_account_types < 1000
)
SELECT account_account_type, num_account_types
FROM daily
