WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(txn_time AS DATE) AS txn_time_day
    FROM transactions
    WHERE merchant IS NOT DISTINCT FROM 'Shell'
      AND txn_time > DATE '2023-07-16'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        txn_time_day,
        SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(txn_type) AS txn_type_length,
        SUBSTRING(merchant, 1, 3) AS merchant_prefix
    FROM islands
    WHERE txn_time >= TO_DATE('2023-08-18')
      AND session_id <= 7
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_txn_time
    FROM cleaned
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        txn_time_day,
        session_id,
        txn_type_length,
        SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_id,
            txn_type,
            session_id
        FROM islands_transactions
    ) src
    PIVOT (
        SUM(session_id)
        FOR txn_type IN ('CREDIT', 'REFUND', 'FEE')
    )
)
SELECT
    s.account_id,
    s.FEE,
    (SELECT MIN(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS min_transaction_amount
FROM wide s
