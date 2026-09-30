WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-06-12 12:00:00'
      AND merchant IS DISTINCT FROM 'Amazon'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER w_ordered AS amount_avg_6,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY is_credit DESC, id) AS event_seq
    FROM latest
    WINDOW w_ordered AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        is_credit,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        amount_avg_6 - is_credit AS amount_avg_6_minus_is_credit
    FROM metrics
    WHERE txn_time >= TO_DATE('2023-06-21')
      AND merchant IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged_flag,
        amount_avg_6_minus_is_credit,
        SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Join a dimension and aggregate
SELECT
    a.account_type,
    COUNT(*) AS cnt,
    MAX(src.is_flagged_flag) AS max_is_flagged_flag,
    GROUPING_ID() AS level_id
FROM sessionized src
JOIN accounts a ON src.account_id = a.id
GROUP BY CUBE (a.account_type);
