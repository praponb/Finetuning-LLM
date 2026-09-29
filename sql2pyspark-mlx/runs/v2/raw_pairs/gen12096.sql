SELECT
    status,
    order_id,
    avg_cost,
    cost_range
FROM (
    SELECT
        status,
        order_id,
        ROUND(AVG(cost), 2) AS avg_cost,
        MAX(cost) - MIN(cost) AS cost_range
    FROM shipments
    WHERE carrier = 'FedEx'
    GROUP BY status, order_id
) AS agg
WHERE avg_cost < 65
ORDER BY status DESC, order_id
LIMIT 10;
