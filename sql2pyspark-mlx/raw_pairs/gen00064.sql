SELECT carrier, ups_count, cost_count
FROM (
    SELECT carrier, SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END) AS ups_count, COUNT(cost) AS cost_count
    FROM shipments
    GROUP BY carrier
) AS sub
WHERE ups_count >= 23
ORDER BY ups_count DESC, carrier
LIMIT 5;
