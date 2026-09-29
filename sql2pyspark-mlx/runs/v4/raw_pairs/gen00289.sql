WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        COALESCE(amount, 0) AS amount_filled
    FROM transactions
    WHERE merchant IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY account_id, txn_type ORDER BY txn_time NULLS FIRST) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS row_seq,
        NTILE(4) OVER (PARTITION BY merchant ORDER BY amount_filled DESC, id) AS amount_filled_bucket
    FROM latest
),
valid_transactions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        amount_filled_bucket - amount_filled AS amount_filled_bucket_minus_amount_filled,
        TRIM(txn_type) AS clean_txn_type,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM enriched
    WHERE txn_time > DATE '2024-01-30'
      AND merchant IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(txn_type, ', ', merchant) AS txn_type_merchant,
        COALESCE(amount_filled_bucket, 0) AS amount_filled_bucket_filled,
        COALESCE(merchant, 'DIRECT') AS merchant_clean
    FROM valid_transactions
    WHERE merchant IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT account_id, id
    FROM filtered
    EXCEPT
    SELECT account_id, id
    FROM transactions
    WHERE txn_time >= '2024-02-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.id = k.id
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT e.*, a.opened_date
    FROM verified e
    INNER JOIN accounts a
        ON e.account_id = a.id
)
-- Grouped metrics
SELECT
    CASE WHEN row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    CASE WHEN amount_filled_bucket_minus_amount_filled >= 70 THEN 'LARGE' ELSE 'SMALL' END AS amount_filled_bucket_minus_amount_filled_tier,
    COUNT_IF(txn_type = 'FEE') AS fee_events,
    MIN(txn_time) AS first_txn_time,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN txn_type = 'REFUND' THEN amount_filled_bucket_filled ELSE 0 END) AS refund_amount_filled_bucket_filled,
    AVG(row_seq) FILTER (WHERE txn_type = 'REFUND') AS refund_avg_row_seq,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount
FROM enriched_transactions
GROUP BY CASE WHEN row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN amount_filled_bucket_minus_amount_filled >= 70 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY amount_filled_bucket_minus_amount_filled_tier DESC
