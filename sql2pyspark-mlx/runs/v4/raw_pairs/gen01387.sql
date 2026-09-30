SELECT
    DATE_TRUNC('QUARTER', start_date) AS quarter_start,
    plan,
    customer_id,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    SIZE(COLLECT_SET(status)) AS num_status,
    APPROX_PERCENTILE(monthly_fee, 0.5) AS monthly_fee_p50
FROM subscriptions
GROUP BY DATE_TRUNC('QUARTER', start_date), plan, customer_id
