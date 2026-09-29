SELECT status, total_cost
FROM (
    SELECT status, SUM(cost) AS total_cost
    FROM shipments
    WHERE carrier = 'Flash'
    GROUP BY status
) AS sub
WHERE total_cost < 480
ORDER BY total_cost DESC
