WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        TO_DATE(txn_time) AS txn_time_date
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-06-06 16:00:00'
),
cleaned AS (
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        txn_time_date,
        DATEDIFF(CURRENT_DATE(), txn_time) / 30 AS months_since_txn_time,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM valid
    WHERE merchant IS NOT NULL
      AND merchant = 'Lazada'
),
flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_transactions AS (
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        txn_time_date,
        months_since_txn_time,
        SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_transactions
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT *
FROM (
    SELECT
        id,
        merchant,
        amount,
        months_since_txn_time
    FROM islands
) src
UNPIVOT (
    value FOR measure IN (amount AS AMOUNT, months_since_txn_time AS MONTHS_SINCE_TXN_TIME)
);
