SELECT *
FROM (
    SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, SUM(cost) AS total_cost
    FROM shipments
    GROUP BY DATE_FORMAT(shipped_date, 'yyyy-MM')
) AS t
WHERE total_cost > 460
ORDER BY total_cost DESC
