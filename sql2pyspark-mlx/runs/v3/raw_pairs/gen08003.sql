SELECT
    carrier,
    status,
    ROUND(AVG(cost), 2) avg_cost,
    SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END) kerry_count,
    SORT_ARRAY(COLLECT_SET(status)) status_list
FROM shipments
WHERE status = 'IN_TRANSIT' AND status IN ('LOST', 'IN_TRANSIT')
GROUP BY ROLLUP(carrier, status);
