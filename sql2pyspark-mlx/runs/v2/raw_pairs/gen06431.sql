SELECT
    channel,
    CASE WHEN amount >= 1320 THEN 'high' WHEN amount >= 760 THEN 'mid' ELSE 'low' END AS amount_bucket,
    COUNT(amount) AS amount_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM orders
GROUP BY channel, amount_bucket
