SELECT *
FROM (
    SELECT status, SUM(cost) AS total_cost, MAX(cost) AS max_cost
    FROM shipments
    GROUP BY status
) AS s
WHERE total_cost > 160
ORDER BY total_cost DESC, status
LIMIT 3;
