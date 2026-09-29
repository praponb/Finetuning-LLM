SELECT carrier, status, cost_range
FROM (
    SELECT carrier, status, MAX(cost) - MIN(cost) AS cost_range
    FROM shipments
    WHERE carrier != 'Flash'
    GROUP BY carrier, status
) AS s
WHERE cost_range < 25
ORDER BY carrier
