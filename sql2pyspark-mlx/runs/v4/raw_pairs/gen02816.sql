WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        UPPER(txn_type) AS txn_type_upper,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= DATE '2023-10-14'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        txn_type_upper,
        resolved_merchant,
        RANK() OVER w_ordered AS amount_rank,
        LAST_VALUE(amount, TRUE) OVER w_ordered_2 AS last_known_amount
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY amount), w_ordered_2 AS (PARTITION BY account_id, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY amount, id) = 1
)
-- Multi-level aggregation
SELECT
    txn_type,
    COUNT_IF(merchant = 'Grab') AS grab_events,
    MIN(last_known_amount) AS min_last_known_amount,
    MAX(last_known_amount) AS max_last_known_amount,
    SUM(CASE WHEN merchant = 'Grab' THEN amount_rank ELSE 0 END) AS grab_amount_rank,
    AVG(amount) AS avg_amount,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
FROM deduped
GROUP BY txn_type
ORDER BY txn_type DESC NULLS FIRST
