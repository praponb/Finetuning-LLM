SELECT order_id, stddev_cost, flash_count
FROM (
    SELECT order_id, ROUND(STDDEV(cost), 2) AS stddev_cost, SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END) AS flash_count
    FROM shipments
    WHERE status <> 'RETURNED'
    GROUP BY order_id
) AS s
WHERE flash_count > 28
ORDER BY stddev_cost DESC;
