SELECT * FROM (SELECT status, carrier, cost FROM shipments)
PIVOT (COUNT(cost) FOR carrier IN ('Kerry', 'UPS'))
ORDER BY status;
