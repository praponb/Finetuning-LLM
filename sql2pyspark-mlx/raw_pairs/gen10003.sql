WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        IF(is_flagged, 'Yes', 'No') is_flagged_label,
        COALESCE(merchant, 'N/A') resolved_merchant
    FROM transactions
    WHERE txn_time > TO_DATE('2023-04-19')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        is_flagged_label,
        resolved_merchant,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount,
        LEAD(amount, 2, 0) OVER (PARTITION BY account_id ORDER BY amount, id) next_amount,
        NTILE(3) OVER (PARTITION BY merchant ORDER BY amount, id) amount_bucket,
        DENSE_RANK() OVER w_grp amount_dense_rank
    FROM valid
    WINDOW w_grp AS (PARTITION BY account_id ORDER BY amount)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        is_flagged_label,
        resolved_merchant,
        amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY first_known_amount DESC, id) row_seq,
        ROUND(first_known_amount / SUM(first_known_amount) OVER (PARTITION BY account_id), 4) first_known_amount_share
    FROM metrics
),
valid_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        is_flagged_label,
        resolved_merchant,
        amount_bucket,
        row_seq,
        first_known_amount_share,
        CASE WHEN row_seq >= 8 THEN 'High' WHEN row_seq >= 4 THEN 'Medium' ELSE 'Low' END row_seq_level,
        GREATEST(amount_bucket, first_known_amount_share) greatest_amount_bucket_first_known_amount_share
    FROM enriched
    WHERE row_seq BETWEEN 7 AND 8
)
-- Join a dimension and aggregate
SELECT
    dim.status,
    COUNT(*) AS rows_total,
    ROUND(SUM(s.greatest_amount_bucket_first_known_amount_share), 2) AS total_greatest_amount_bucket_first_known_amount_share,
    GROUPING_ID() AS gid
FROM valid_transactions s
INNER JOIN accounts dim ON s.account_id = dim.id
GROUP BY ROLLUP (dim.status)
ORDER BY total_greatest_amount_bucket_first_known_amount_share NULLS LAST;
