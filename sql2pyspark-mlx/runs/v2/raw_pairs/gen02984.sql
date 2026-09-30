SELECT
    merchant,
    is_flagged,
    MAX(amount) AS max_amount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
FROM transactions
WHERE HOUR(txn_time) BETWEEN 6 AND 9
GROUP BY CUBE(merchant, is_flagged)
ORDER BY max_amount;
