SELECT txn_month, stddev_amount, merchant_values
FROM (
    SELECT MONTH(txn_time) AS txn_month, ROUND(STDDEV(amount), 2) AS stddev_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
    FROM transactions
    GROUP BY MONTH(txn_time)
) AS agg
WHERE stddev_amount >= 39
ORDER BY stddev_amount, txn_month
LIMIT 50
