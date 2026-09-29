WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), txn_time) / 7 AS weeks_since_txn_time,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE merchant NOT IN ('Amazon', 'Grab')
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    SUM(CASE WHEN txn_type = 'REFUND' THEN amount ELSE 0 END) AS refund_amount,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
FROM base
GROUP BY DATE_TRUNC('WEEK', txn_time)
ORDER BY week_start;
