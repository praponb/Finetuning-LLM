SELECT
    merchant,
    QUARTER(txn_time) txn_quarter,
    COUNT(amount) amount_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) merchant_values
FROM transactions
WHERE amount BETWEEN 2100 AND 3400
GROUP BY merchant, QUARTER(txn_time)
HAVING SUM(amount) >= 7000;
