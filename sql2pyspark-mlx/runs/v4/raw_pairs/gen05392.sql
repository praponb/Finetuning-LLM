WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id ASC) = 1
)
-- Multi-level aggregation
SELECT
    merchant,
    COUNT(DISTINCT merchant) AS distinct_merchants,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    COUNT_IF(txn_type = 'DEBIT') AS debit_events
FROM ranked
GROUP BY 1;
