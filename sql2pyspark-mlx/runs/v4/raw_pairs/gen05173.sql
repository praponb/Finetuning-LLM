SELECT
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    CASE WHEN cost >= 105 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    AVG(cost) AS avg_cost,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(status)) AS num_status,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    SIZE(COLLECT_SET(status)) AS num_status_2
FROM shipments
GROUP BY 1, 2
HAVING avg_cost >= 0
ORDER BY num_status NULLS LAST;
