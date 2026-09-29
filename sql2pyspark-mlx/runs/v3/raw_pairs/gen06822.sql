SELECT *
FROM (
    SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values, ROUND((100 * SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_flash
    FROM shipments
    GROUP BY status
) AS sub
WHERE pct_flash < 27
ORDER BY status
LIMIT 10
