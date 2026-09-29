SELECT status, avg_cost, status_list
FROM (
    SELECT status, AVG(cost) AS avg_cost, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM shipments
    WHERE delivered_date > '2023-09-01'
    GROUP BY status
) AS s
WHERE avg_cost >= 52
ORDER BY avg_cost DESC
