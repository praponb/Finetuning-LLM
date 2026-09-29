WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        COALESCE(amount, 0) AS amount_filled
    FROM transactions
    WHERE txn_time > DATE '2023-07-30'
      AND merchant IS NOT NULL
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        p.opened_date,
        p.status
    FROM cleaned src
    INNER JOIN accounts p
        ON src.account_id = p.id
    WHERE p.balance BETWEEN 42000 AND 61000
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.account_type,
        a.opened_date AS account_opened_date
    FROM joined s
    INNER JOIN accounts a
        ON s.account_id = a.id
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) AS prev_txn_time
    FROM with_dims
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_transactions
),
sessions_transactions_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', account_opened_date) AS activity_month,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows,
    ROUND(AVG(is_new_session), 2) AS avg_is_new_session,
    COUNT(DISTINCT status) AS distinct_status,
    SUM(CASE WHEN status = 'CLOSED' THEN is_new_session ELSE 0 END) AS closed_is_new_session
FROM sessions_transactions_2
GROUP BY DATE_TRUNC('MONTH', account_opened_date)
HAVING COUNT(DISTINCT status) > 1
ORDER BY avg_is_new_session DESC;
