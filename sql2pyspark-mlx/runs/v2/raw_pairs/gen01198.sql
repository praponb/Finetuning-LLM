SELECT status, unique_orders, min_cost
FROM (
    SELECT status, COUNT(DISTINCT cost) unique_orders, MIN(cost) min_cost
    FROM shipments
    GROUP BY status
) s
WHERE min_cost > 29
