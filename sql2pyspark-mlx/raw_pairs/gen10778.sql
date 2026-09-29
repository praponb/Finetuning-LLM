WITH src AS (
    SELECT *
    FROM shipments
    WHERE YEAR(delivered_date) = 2024
),
status_summary AS (
    SELECT status, MAX(cost) AS max_cost, AVG(cost) AS avg_cost
    FROM src
    GROUP BY status
)
SELECT *
FROM status_summary
WHERE avg_cost >= 67
ORDER BY max_cost ASC, status ASC
LIMIT 5;
