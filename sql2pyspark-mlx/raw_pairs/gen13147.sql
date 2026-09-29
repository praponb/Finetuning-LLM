WITH kept_keys AS (
    -- Set difference on keys
    SELECT txn_type
    FROM transactions
    EXCEPT
    SELECT txn_type
    FROM transactions
    WHERE amount < 2600
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM transactions s
    INNER JOIN kept_keys k ON s.txn_type = k.txn_type
)
-- Aggregate with conditional metrics
SELECT
    txn_type,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    STDDEV_SAMP(amount) AS amount_stddev,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types
FROM kept_rows
GROUP BY 1
HAVING STDDEV_SAMP(amount) >= 0
ORDER BY txn_type DESC
