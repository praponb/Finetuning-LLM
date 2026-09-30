SELECT
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    STDDEV_SAMP(cost) AS cost_stddev,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    COUNT(DISTINCT carrier) AS distinct_carriers
FROM shipments
GROUP BY 1;
