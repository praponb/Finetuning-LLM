SELECT
    status,
    carrier,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM shipments
WHERE shipped_date < '2025-06-30' AND MONTH(delivered_date) IN (1, 8, 10) AND status = 'RETURNED'
GROUP BY status, carrier
HAVING COUNT(cost) >= 1
ORDER BY carrier, status DESC;
