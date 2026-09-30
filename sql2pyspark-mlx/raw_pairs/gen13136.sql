SELECT
    TO_DATE(txn_time) txn_day,
    merchant,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) txn_type_values,
    ROUND((100 * SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_7_eleven
FROM transactions
GROUP BY TO_DATE(txn_time), merchant
HAVING ROUND((100 * SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 62
ORDER BY pct_7_eleven DESC;
