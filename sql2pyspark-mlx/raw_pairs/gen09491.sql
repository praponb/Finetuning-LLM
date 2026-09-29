SELECT
    carrier,
    order_id,
    APPROX_PERCENTILE(cost, 0.9) AS cost_p90,
    MAX(cost) AS max_cost,
    COUNT(DISTINCT status) AS distinct_status,
    COUNT(CASE WHEN status = 'LOST' THEN 1 END) AS lost_count,
    PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY COUNT(DISTINCT status) DESC) AS distinct_status_percent_rank
FROM shipments
GROUP BY carrier, order_id
ORDER BY order_id DESC
