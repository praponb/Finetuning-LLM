SELECT
    CASE WHEN cost >= 94 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(cost) FILTER (WHERE carrier = 'DHL') AS dhl_max_cost,
    STDDEV_SAMP(cost) AS cost_stddev,
    COUNT(*) AS num_rows,
    GROUPING(CASE WHEN cost >= 94 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM shipments
GROUP BY CUBE (CASE WHEN cost >= 94 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', delivered_date))
ORDER BY cost_stddev ASC
