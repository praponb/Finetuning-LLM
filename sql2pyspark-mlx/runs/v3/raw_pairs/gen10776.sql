SELECT
    CASE WHEN amount >= 1470 THEN 'high' WHEN amount >= 790 THEN 'mid' ELSE 'low' END AS amount_bucket,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_amount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM payments
GROUP BY 1
HAVING stddev_amount >= 46
