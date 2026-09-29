WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(txn_type)) txn_type_title,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END is_flagged_label,
        COALESCE(merchant, 'N/A') merchant_filled
    FROM transactions
    WHERE NOT is_flagged
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.txn_type,
        e.txn_time,
        e.merchant,
        e.is_flagged,
        e.txn_type_title,
        e.is_flagged_label,
        e.merchant_filled,
        b.balance,
        b.opened_date
    FROM cleaned e
    INNER JOIN accounts b
        ON e.account_id = b.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT account_id, id
    FROM with_dims
    INTERSECT ALL
    SELECT account_id, id
    FROM transactions
    WHERE merchant IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, COUNT(balance) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) balance_count_30d
    FROM verified
)
-- Aggregate with conditional metrics
SELECT
    merchant,
    MIN(balance_count_30d) AS min_balance_count_30d,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    MIN(txn_time) AS first_txn_time,
    SUM(CASE WHEN merchant = 'Amazon' THEN balance ELSE 0 END) AS amazon_balance,
    PERCENTILE_APPROX(balance, 0.5) AS balance_p50
FROM enriched
GROUP BY merchant
ORDER BY amazon_balance
