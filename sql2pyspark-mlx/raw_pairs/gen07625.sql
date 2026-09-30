SELECT
    customer_id,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    STDDEV_SAMP(amount) AS amount_stddev,
    STDDEV_SAMP(amount) AS amount_stddev_2,
    STDDEV_SAMP(discount) AS discount_stddev,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95
FROM orders
GROUP BY customer_id
ORDER BY amount_p95
