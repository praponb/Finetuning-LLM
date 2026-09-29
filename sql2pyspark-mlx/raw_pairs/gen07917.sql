SELECT
    CASE WHEN amount >= 3100 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    txn_type,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95,
    COUNT_IF(merchant = 'Shell') AS shell_events,
    APPROX_PERCENTILE(amount, 0.5) AS amount_p50,
    SUM(CASE WHEN merchant = 'Lazada' THEN amount ELSE 0 END) AS lazada_amount,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
FROM transactions
GROUP BY CASE WHEN amount >= 3100 THEN 'HIGH' ELSE 'LOW' END, txn_type
