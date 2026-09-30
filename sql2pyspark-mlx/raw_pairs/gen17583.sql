WITH filtered AS (
    SELECT
        *,
        LENGTH(merchant) AS merchant_length,
        TRIM(merchant) AS clean_merchant,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE merchant IS NOT NULL
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    merchant_length,
    NTILE(5) OVER (PARTITION BY txn_type, account_id ORDER BY merchant_length DESC, id) AS merchant_length_bucket,
    ROW_NUMBER() OVER w_ordered AS seq_no,
    SUM(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d,
    AVG(amount) OVER w_ordered_2 AS amount_avg_2_prior
FROM filtered
WINDOW w_ordered AS (ORDER BY txn_time DESC, id), w_ordered_2 AS (ORDER BY merchant_length, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
