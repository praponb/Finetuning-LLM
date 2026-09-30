SELECT
    CASE WHEN merchant IN ('7-Eleven', 'Starbucks') THEN 'core' ELSE 'other' END AS merchant_group,
    SUM(CASE WHEN merchant = 'Grab' THEN amount ELSE 0 END) AS grab_amount,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
FROM transactions
WHERE (merchant = 'Shell' AND amount < 3100) OR is_flagged = TRUE
GROUP BY CASE WHEN merchant IN ('7-Eleven', 'Starbucks') THEN 'core' ELSE 'other' END
HAVING ROUND(STDDEV(amount), 2) >= 27
ORDER BY merchant_group;
