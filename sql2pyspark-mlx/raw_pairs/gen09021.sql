SELECT
    CASE WHEN amount >= 4300 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END), 0), 2) AS amount_per_fee,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count,
    STDDEV_SAMP(amount) AS amount_stddev
FROM transactions
GROUP BY CASE WHEN amount >= 4300 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY amount_stddev DESC NULLS LAST;
