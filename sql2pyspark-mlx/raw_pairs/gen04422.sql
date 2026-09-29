WITH base AS (
    SELECT
        id,
        account_id,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        HOUR(txn_time) AS txn_time_hour
    FROM transactions
    WHERE merchant IS NOT NULL
),
enriched AS (
    SELECT
        *,
        LAG(is_flagged_flag) OVER w_ordered AS prev_is_flagged_flag,
        LEAD(txn_time_hour) OVER w_ordered AS next_txn_time_hour,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM base
    WHERE is_flagged
    WINDOW w_ordered AS (PARTITION BY merchant, account_id ORDER BY txn_time_hour, id)
),
kept_keys AS (
    SELECT id, merchant
    FROM enriched
    EXCEPT
    SELECT id, merchant
    FROM transactions
    WHERE amount < 2000
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.merchant = k.merchant
),
cleaned AS (
    SELECT
        *,
        is_flagged_flag / NULLIF(txn_time_hour, 0) AS is_flagged_flag_per_txn_time_hour,
        CEIL(next_txn_time_hour) AS next_txn_time_hour_whole,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM verified
    WHERE merchant IS DISTINCT FROM 'Grab'
      AND txn_time_hour <= 47
),
metrics AS (
    SELECT *, PERCENT_RANK() OVER (PARTITION BY merchant, account_id ORDER BY is_flagged_flag DESC NULLS FIRST) AS is_flagged_flag_percent_rank
    FROM cleaned
),
enriched_transactions AS (
    SELECT
        id,
        is_flagged,
        is_flagged_flag,
        txn_time_hour,
        prev_is_flagged_flag,
        is_flagged_flag_percent_rank,
        COUNT(is_flagged_flag) OVER (PARTITION BY account_id) AS account_id_count_is_flagged_flag,
        DENSE_RANK() OVER (ORDER BY prev_is_flagged_flag NULLS FIRST) AS prev_is_flagged_flag_dense_rank,
        DENSE_RANK() OVER (PARTITION BY account_id, merchant ORDER BY row_seq) AS row_seq_dense_rank,
        MIN(txn_time_hour) OVER (PARTITION BY merchant) AS merchant_min_txn_time_hour
    FROM metrics
),
filtered AS (
    SELECT
        id,
        is_flagged,
        is_flagged_flag,
        txn_time_hour,
        prev_is_flagged_flag,
        is_flagged_flag_percent_rank,
        account_id_count_is_flagged_flag,
        prev_is_flagged_flag_dense_rank,
        merchant_min_txn_time_hour,
        CASE WHEN account_id_count_is_flagged_flag >= 13 THEN 'High' WHEN account_id_count_is_flagged_flag >= 8 THEN 'Medium' ELSE 'Low' END AS account_id_count_is_flagged_flag_tier
    FROM enriched_transactions
    WHERE is_flagged
)
SELECT
    id,
    is_flagged,
    prev_is_flagged_flag,
    is_flagged_flag_percent_rank,
    prev_is_flagged_flag_dense_rank
FROM filtered;
