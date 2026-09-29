WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount * 1.15, 2) AS amount_adjusted,
        CASE WHEN merchant IN ('Starbucks', 'Shell') THEN 'Priority' ELSE 'Other' END AS merchant_group
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time >= '2024-02-16'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        amount_adjusted,
        LAG(amount_adjusted) OVER w_ordered AS prev_amount_adjusted,
        LAG(amount) OVER w_ordered AS prev_amount,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        SUM(amount) OVER (PARTITION BY merchant, account_id) AS merchant_sum_amount
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY account_id ORDER BY amount_adjusted, id)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        is_flagged,
        amount_adjusted,
        prev_amount_adjusted,
        prev_amount,
        merchant_sum_amount,
        prev_amount - amount_adjusted AS prev_amount_minus_amount_adjusted
    FROM enriched
    WHERE amount_adjusted < 27
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT txn_type, account_id
    FROM base
    INTERSECT ALL
    SELECT txn_type, account_id
    FROM transactions
    WHERE txn_time < '2025-03-09'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.txn_type = k.txn_type AND s.account_id = k.account_id
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 3500 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    CASE WHEN prev_amount >= 3800 THEN 'LARGE' ELSE 'SMALL' END AS prev_amount_tier,
    account_id,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    MAX(prev_amount_minus_amount_adjusted) FILTER (WHERE txn_type = 'DEBIT') AS debit_max_prev_amount_minus_amount_adjusted,
    RANK() OVER (PARTITION BY CASE WHEN amount >= 3500 THEN 'LARGE' ELSE 'SMALL' END ORDER BY MAX(prev_amount_minus_amount_adjusted) FILTER (WHERE txn_type = 'DEBIT') DESC) AS debit_max_prev_amount_minus_amount_adjusted_rank
FROM kept_rows
GROUP BY 1, 2, 3
HAVING debit_max_prev_amount_minus_amount_adjusted > 0
ORDER BY distinct_txn_types DESC;
