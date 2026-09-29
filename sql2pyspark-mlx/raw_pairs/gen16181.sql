WITH with_windows AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY merchant ORDER BY amount, id) AS amount_bucket,
        AVG(amount) OVER (PARTITION BY txn_type) AS txn_type_avg_amount,
        MAX(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d,
        MAX(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d_2,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM transactions
),
metrics AS (
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY txn_type ORDER BY amount, id) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY txn_type ORDER BY amount, id) AS next_amount,
        LAG(amount_max_90d_2) OVER (PARTITION BY txn_type ORDER BY amount, id) AS prev_amount_max_90d_2,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS row_seq
    FROM with_windows
    WHERE account_id IN (6, 28, 40)
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
        amount_bucket,
        amount_max_90d,
        amount_max_90d_2,
        cumulative_amount,
        prev_amount,
        next_amount,
        prev_amount_max_90d_2,
        row_seq,
        COALESCE(amount_bucket, 0) AS amount_bucket_filled
    FROM metrics
    WHERE merchant IS NOT NULL
      AND prev_amount >= 3000
),
filtered AS (
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        amount_bucket,
        amount_max_90d,
        amount_max_90d_2,
        cumulative_amount,
        prev_amount,
        next_amount,
        prev_amount_max_90d_2,
        row_seq,
        amount_bucket_filled,
        CONCAT_WS('/', txn_type, merchant) AS txn_type_merchant_key,
        INITCAP(TRIM(merchant)) AS merchant_title
    FROM base
    WHERE merchant IS NOT NULL
),
with_windows_transactions AS (
    SELECT
        id,
        txn_time,
        is_flagged,
        amount_max_90d,
        amount_max_90d_2,
        cumulative_amount,
        next_amount,
        row_seq,
        amount_bucket_filled,
        txn_type_merchant_key,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY prev_amount_max_90d_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        MAX(amount) OVER (PARTITION BY merchant ORDER BY prev_amount_max_90d_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        COUNT(amount) OVER (PARTITION BY merchant ORDER BY prev_amount_max_90d_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        MIN(amount) OVER (PARTITION BY merchant ORDER BY prev_amount_max_90d_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        ROW_NUMBER() OVER (PARTITION BY MONTH(txn_time), merchant ORDER BY amount_bucket, id) AS event_seq
    FROM filtered
)
SELECT
    id,
    txn_time,
    is_flagged,
    amount_max_90d,
    amount_max_90d_2,
    cumulative_amount,
    next_amount,
    amount_bucket_filled,
    running_max_amount
FROM with_windows_transactions
ORDER BY amount_max_90d_2 DESC NULLS LAST, id;
