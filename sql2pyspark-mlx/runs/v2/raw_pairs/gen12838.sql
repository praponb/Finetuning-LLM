SELECT
    status,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank
FROM shipments
GROUP BY status
