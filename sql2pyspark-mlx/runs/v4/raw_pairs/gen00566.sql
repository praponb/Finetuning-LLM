WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-06-14 06:00:00'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount, 2) OVER (PARTITION BY txn_type ORDER BY amount, id) AS next_amount,
        COUNT(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
        COUNT(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d_2,
        LAST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant
    FROM cleaned
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS LAST, id) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
),
cleaned_transactions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        amount_count_30d,
        amount_count_30d_2,
        last_known_merchant,
        latest_rank,
        CASE WHEN amount >= 3100 THEN 'High' WHEN amount >= 1700 THEN 'Medium' ELSE 'Low' END AS amount_band,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM deduped
    WHERE txn_time > DATE '2024-01-03'
)
SELECT
    id,
    txn_time,
    merchant,
    amount_count_30d,
    amount_count_30d_2,
    last_known_merchant,
    latest_rank,
    amount_band
FROM cleaned_transactions;
