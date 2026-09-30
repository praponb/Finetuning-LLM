WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        CAST(txn_time AS DATE) AS txn_time_day,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-12-31 23:00:00')
)
SELECT
    *,
    ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type, account_id), 4) AS amount_share,
    MAX(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d,
    AVG(is_flagged_flag) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_flagged_flag_avg_30d
FROM valid;
