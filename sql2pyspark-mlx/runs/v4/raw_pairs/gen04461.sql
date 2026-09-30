WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS LAST, id) AS rn
    FROM transactions
    QUALIFY rn = 1
),
cleaned AS (
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        SUBSTRING(merchant, 1, 3) AS merchant_prefix,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM deduped
    WHERE merchant = 'Grab'
),
deduped_transactions AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time NULLS FIRST, id) = 1
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY merchant, account_id ORDER BY txn_time, id) AS row_seq,
        ROW_NUMBER() OVER w_part AS seq_no,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant
    FROM deduped_transactions
    WINDOW w_part AS (PARTITION BY merchant ORDER BY txn_time, id)
),
remaining_keys AS (
    SELECT account_id
    FROM metrics
    INTERSECT
    SELECT account_id
    FROM transactions
    WHERE is_flagged = FALSE
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id
),
filtered AS (
    SELECT
        id,
        account_id,
        merchant,
        is_flagged,
        merchant_prefix,
        row_seq,
        first_known_merchant,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM verified
    WHERE account_id IN (1, 13, 16)
),
base AS (
    SELECT
        id,
        merchant,
        is_flagged,
        merchant_prefix,
        first_known_merchant,
        seq_no_filled,
        CASE WHEN merchant IN ('Lazada', 'Amazon') THEN 'Priority' ELSE 'Other' END AS merchant_group
    FROM filtered
    WHERE row_seq BETWEEN 6 AND 7
)
SELECT
    merchant,
    CASE WHEN seq_no_filled >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_filled_tier,
    PERCENTILE_APPROX(seq_no_filled, ARRAY(0.25, 0.5, 0.75)) AS seq_no_filled_quartiles,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    ROUND(AVG(seq_no_filled), 2) AS avg_seq_no_filled,
    COUNT_IF(merchant = 'Starbucks') AS starbucks_events
FROM base
GROUP BY GROUPING SETS ((merchant, CASE WHEN seq_no_filled >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END), (merchant), ())
HAVING avg_seq_no_filled > 0
ORDER BY merchant;
