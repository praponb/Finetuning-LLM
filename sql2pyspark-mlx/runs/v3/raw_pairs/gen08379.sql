SELECT
    HOUR(txn_time) AS txn_hour,
    txn_type,
    DATE_TRUNC('MONTH', txn_time) AS txn_month,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
FROM transactions
GROUP BY HOUR(txn_time), txn_type, DATE_TRUNC('MONTH', txn_time)
ORDER BY txn_month DESC, txn_hour DESC, txn_type
LIMIT 20
