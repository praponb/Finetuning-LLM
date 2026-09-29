WITH filtered AS (
    SELECT *, DATE_FORMAT(txn_time, 'MMM yyyy') AS txn_time_label
    FROM transactions
    WHERE txn_time >= DATE '2023-09-22'
      AND merchant IS DISTINCT FROM 'Grab'
),
base AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_time_label,
        CEIL(amount) AS amount_whole,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM filtered
    WHERE txn_time >= TO_TIMESTAMP('2023-11-03 22:00:00')
      AND merchant IS NOT DISTINCT FROM 'Lazada'
),
with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    SELECT
        id,
        txn_type,
        txn_time,
        amount_whole,
        SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
base_transactions AS (
    SELECT
        id,
        txn_type,
        txn_time,
        amount_whole,
        amount_whole - island_id AS amount_whole_minus_island_id
    FROM islands
    WHERE txn_type = 'CREDIT'
)
SELECT
    id,
    txn_type,
    txn_time,
    LEAD(amount_whole) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) AS next_amount_whole,
    LAG(amount_whole) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) AS prev_amount_whole,
    LEAD(amount_whole_minus_island_id) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) AS next_amount_whole_minus_island_id,
    LAG(amount_whole_minus_island_id) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) AS prev_amount_whole_minus_island_id,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) AS seq_no
FROM base_transactions
WHERE TO_DATE(txn_time) < '2024-03-04';
