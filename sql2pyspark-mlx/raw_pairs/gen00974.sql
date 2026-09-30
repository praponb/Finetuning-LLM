WITH with_windows AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount,
        AVG(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_avg_90d,
        COUNT(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_count_90d,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_merchant,
        NTILE(4) OVER (PARTITION BY merchant ORDER BY amount, id) amount_bucket
    FROM transactions
)
SELECT
    account_id,
    COUNT(DISTINCT merchant) AS distinct_merchants,
    COUNT(*) AS num_rows,
    COUNT_IF(txn_type = 'FEE') AS fee_events
FROM with_windows
GROUP BY account_id;
