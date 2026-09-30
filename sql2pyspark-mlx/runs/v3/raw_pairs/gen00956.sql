SELECT
    DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month,
    is_flagged,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values,
    COUNT(amount) AS amount_count
FROM transactions
GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM'), is_flagged
