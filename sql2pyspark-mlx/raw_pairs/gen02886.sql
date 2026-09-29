SELECT
    status,
    YEAR(delivered_date) AS delivered_year,
    ROUND(AVG(cost), 2) AS avg_cost,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM shipments
GROUP BY status, YEAR(delivered_date)
ORDER BY avg_cost DESC;
