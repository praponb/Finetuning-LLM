SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    customer_id,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    SUM(balance) AS total_balance,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    ROUND(AVG(balance), 2) AS avg_balance,
    GROUPING_ID() AS grouping_id
FROM accounts
GROUP BY ROLLUP (DATE_TRUNC('MONTH', opened_date), customer_id)
ORDER BY avg_balance
