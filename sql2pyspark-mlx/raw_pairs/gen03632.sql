SELECT
    status,
    COUNT_IF(status = 'RETURNED') AS returned_events,
    STDDEV_SAMP(amount) AS amount_stddev,
    CORR(discount, amount) AS corr_discount_amount,
    APPROX_PERCENTILE(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    SIZE(COLLECT_SET(channel)) AS num_channels
FROM orders
GROUP BY ROLLUP (status)
ORDER BY status DESC NULLS FIRST
