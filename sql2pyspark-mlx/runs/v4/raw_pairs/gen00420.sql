WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END AS is_starbucks,
        DATE_TRUNC('YEAR', txn_time) AS txn_time_period,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IN ('7-Eleven', 'Shell')
)
-- Window metrics over several different partitions
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    is_starbucks,
    txn_time_period,
    merchant_clean,
    AVG(amount) OVER (ORDER BY is_starbucks, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
    LAST_VALUE(is_starbucks) IGNORE NULLS OVER (PARTITION BY txn_type, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_starbucks,
    AVG(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_90d
FROM filtered
