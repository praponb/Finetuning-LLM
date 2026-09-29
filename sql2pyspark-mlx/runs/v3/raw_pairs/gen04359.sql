SELECT
    txn_type,
    COUNT(DISTINCT amount) AS unique_merchants,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count
FROM transactions
WHERE NOT is_flagged OR merchant NOT IN ('Lazada', 'Shell', '7-Eleven')
GROUP BY txn_type
HAVING COUNT(DISTINCT amount) > 2
ORDER BY 1 DESC
