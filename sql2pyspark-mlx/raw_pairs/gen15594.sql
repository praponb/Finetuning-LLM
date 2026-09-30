SELECT
    CASE WHEN amount >= 1210 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    CASE WHEN discount >= 0.33 THEN 'LARGE' ELSE 'SMALL' END AS discount_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(DISTINCT status) AS distinct_status,
    STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(amount) FILTER (WHERE status = 'RETURNED') AS returned_max_amount
FROM orders
GROUP BY ROLLUP (CASE WHEN amount >= 1210 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN discount >= 0.33 THEN 'LARGE' ELSE 'SMALL' END)
HAVING distinct_customer_ids >= 1
ORDER BY distinct_status;
