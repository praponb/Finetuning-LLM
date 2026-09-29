WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        account_id,
        txn_time,
        merchant
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS LAST, id ASC) = 1
)
-- Aggregate with conditional metrics
SELECT
    merchant,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count,
    COUNT(CASE WHEN merchant = 'Lazada' THEN 1 END) AS lazada_count,
    MAX(txn_time) AS last_seen,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count
FROM latest
GROUP BY GROUPING SETS ((merchant, DATE_TRUNC('MONTH', txn_time)), (merchant))
ORDER BY merchant DESC;
