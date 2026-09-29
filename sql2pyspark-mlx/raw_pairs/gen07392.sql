WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        UPPER(txn_type) AS txn_type_upper,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-09-09 10:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_type,
        merchant,
        txn_type_upper,
        SUM(amount) OVER w_running AS last3_sum_amount,
        AVG(amount) OVER w_running AS last3_avg_amount,
        MAX(amount) OVER w_running AS last3_max_amount,
        MAX(amount) OVER (PARTITION BY merchant, txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d
    FROM cleaned
    WINDOW w_running AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        merchant,
        last3_sum_amount,
        last3_avg_amount,
        last3_max_amount,
        amount_max_30d,
        CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END AS is_debit,
        TRIM(merchant) AS clean_merchant,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM metrics
    WHERE merchant IS NOT NULL
      AND merchant = 'Starbucks'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(is_debit) OVER (PARTITION BY txn_type ORDER BY amount_max_30d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_is_debit,
        MAX(is_debit) OVER (PARTITION BY txn_type ORDER BY amount_max_30d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_is_debit,
        MIN(is_debit) OVER (PARTITION BY txn_type ORDER BY amount_max_30d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_is_debit,
        ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY last3_avg_amount, id) AS row_seq
    FROM filtered
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(last3_sum_amount, 2) OVER (ORDER BY last3_max_amount, id) AS next_last3_sum_amount,
        NTILE(10) OVER w_ordered AS last7_min_is_debit_bucket
    FROM enriched
    WHERE merchant IN ('Grab', '7-Eleven', 'Lazada')
    WINDOW w_ordered AS (PARTITION BY txn_type ORDER BY last7_min_is_debit, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last3_max_amount >= 3100 THEN 'LARGE' ELSE 'SMALL' END AS last3_max_amount_tier,
    CASE WHEN last3_avg_amount >= 3600 THEN 'LARGE' ELSE 'SMALL' END AS last3_avg_amount_tier,
    ROUND(SUM(amount_max_30d) / NULLIF(COUNT(CASE WHEN merchant = 'Grab' THEN 1 END), 0), 2) AS amount_max_30d_per_grab,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    MIN(last3_max_amount) AS min_last3_max_amount
FROM with_windows
GROUP BY CASE WHEN last3_max_amount >= 3100 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last3_avg_amount >= 3600 THEN 'LARGE' ELSE 'SMALL' END
