SELECT
    QUARTER(txn_time) AS txn_quarter,
    txn_type,
    MAX(amount) - MIN(amount) AS amount_range,
    MAX(txn_time) AS last_txn_time,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
FROM transactions
GROUP BY CUBE(QUARTER(txn_time), txn_type)
ORDER BY txn_quarter;
