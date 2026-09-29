SELECT status, pct_ups, min_cost
FROM (
    SELECT status, ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_ups, MIN(cost) min_cost
    FROM shipments
    GROUP BY status
) s
WHERE pct_ups >= 65
ORDER BY pct_ups;
