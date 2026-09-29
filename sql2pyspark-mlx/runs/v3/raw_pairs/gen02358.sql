SELECT status, CASE WHEN carrier IN ('Kerry', 'UPS') THEN 'group_a' ELSE 'other' END AS carrier_group, AVG(cost) AS avg_cost
FROM shipments
WHERE carrier IN ('Flash', 'DHL', 'UPS')
GROUP BY 1, 2;
