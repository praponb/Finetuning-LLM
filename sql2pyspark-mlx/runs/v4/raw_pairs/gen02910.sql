WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        LENGTH(merchant) AS merchant_length,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND amount BETWEEN 1200 AND 3600
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM with_gaps
),
matched_keys AS (
    -- Set difference on keys
    SELECT account_id
    FROM islands
    EXCEPT
    SELECT account_id
    FROM transactions
    WHERE amount < 3700
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN matched_keys k ON s.account_id = k.account_id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id_2 >= 8 THEN 'HIGH' ELSE 'LOW' END AS island_id_2_tier,
    SUM(is_new_burst) AS total_is_new_burst,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types
FROM kept_rows
GROUP BY CUBE (CASE WHEN island_id_2 >= 8 THEN 'HIGH' ELSE 'LOW' END);
