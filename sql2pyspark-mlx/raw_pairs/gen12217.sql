WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(txn_type, 1, 4) AS txn_type_prefix
    FROM transactions
    WHERE is_flagged = FALSE
      AND merchant IS NOT DISTINCT FROM 'Grab'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label,
        CAST(amount AS INT) AS amount_int,
        COALESCE(merchant, 'N/A') AS merchant_filled
    FROM valid
    WHERE txn_time >= DATE '2023-09-24'
      AND merchant IS NOT DISTINCT FROM '7-Eleven'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_type_prefix,
        is_flagged_label,
        amount_int,
        merchant_filled,
        CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END AS is_fee,
        COALESCE(merchant, 'DIRECT') AS merchant_filled_2
    FROM filtered
    WHERE txn_time >= DATE '2023-09-25'
      AND merchant IS NOT DISTINCT FROM 'Shell'
),
valid_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        txn_type_prefix,
        is_flagged_label,
        merchant_filled,
        merchant_filled_2,
        GREATEST(amount_int, amount) AS greatest_amount_int_amount,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label_2,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM base
    WHERE txn_time > TIMESTAMP '2023-07-30 22:00:00'
),
filtered_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        txn_time,
        txn_type_prefix,
        merchant_filled_2,
        greatest_amount_int_amount,
        merchant_clean,
        LENGTH(merchant) AS merchant_length,
        greatest_amount_int_amount * amount AS greatest_amount_int_amount_times_amount,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM valid_transactions
    WHERE merchant = '7-Eleven'
      AND merchant IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount) OVER w_hist AS next_amount,
        LEAD(merchant_length) OVER w_hist AS next_merchant_length,
        ROW_NUMBER() OVER w_hist AS event_seq,
        LAST_VALUE(merchant_length, TRUE) OVER (ORDER BY greatest_amount_int_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant_length
    FROM filtered_transactions
    WHERE amount BETWEEN 1500 AND 2200
    WINDOW w_hist AS (ORDER BY txn_time, id)
)
-- Ranking and gap features
SELECT
    id,
    amount,
    txn_time,
    merchant_clean,
    merchant_length,
    resolved_merchant,
    next_merchant_length,
    event_seq,
    last_known_merchant_length,
    SUM(merchant_length) OVER (ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_merchant_length,
    COUNT(merchant_length) OVER (ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_merchant_length,
    AVG(merchant_length) OVER (ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_merchant_length,
    MAX(merchant_length) OVER (ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_merchant_length
FROM enriched
