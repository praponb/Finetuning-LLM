WITH prev_events AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_transactions AS (
    SELECT
        id,
        account_id,
        txn_time,
        SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    account_id,
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    MIN(txn_time) AS started_at,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    MIN(txn_time) AS started_at_2,
    MAX(txn_time) AS ended_at,
    MAX(txn_time) AS last_txn_time,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    GROUPING(account_id) AS gid
FROM sessions_transactions
GROUP BY GROUPING SETS ((account_id, DATE_TRUNC('QUARTER', txn_time)), (account_id), ())
ORDER BY account_id DESC NULLS LAST;
