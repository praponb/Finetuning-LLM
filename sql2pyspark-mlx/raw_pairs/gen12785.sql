WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        ROUND(amount * 1.15, 2) AS adjusted_amount
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time > DATE '2023-11-11'
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_time,
        a.is_flagged,
        d.opened_date,
        d.status
    FROM valid a
    INNER JOIN accounts d
        ON a.account_id = d.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(session_id) OVER (PARTITION BY account_id ORDER BY opened_date, id) AS prev_session_id,
        LEAD(session_id) OVER (PARTITION BY account_id ORDER BY opened_date, id) AS next_session_id,
        LAG(new_group_flag) OVER (PARTITION BY account_id ORDER BY opened_date, id) AS prev_new_group_flag,
        LEAD(new_group_flag) OVER (PARTITION BY account_id ORDER BY opened_date, id) AS next_new_group_flag,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY opened_date, id) AS event_seq
    FROM sessions_transactions
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_id,
        status,
        new_group_flag
    FROM scored
) src
PIVOT (
    AVG(new_group_flag)
    FOR status IN ('OPEN', 'CLOSED')
);
