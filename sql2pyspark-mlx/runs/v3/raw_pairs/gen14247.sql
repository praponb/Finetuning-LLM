SELECT carrier_group, status, cost_range
FROM (
    SELECT CASE WHEN carrier IN ('UPS', 'DHL') THEN 'core' ELSE 'other' END AS carrier_group, status, MAX(cost) - MIN(cost) AS cost_range
    FROM shipments
    GROUP BY CASE WHEN carrier IN ('UPS', 'DHL') THEN 'core' ELSE 'other' END, status
) AS t
WHERE cost_range > 79;
