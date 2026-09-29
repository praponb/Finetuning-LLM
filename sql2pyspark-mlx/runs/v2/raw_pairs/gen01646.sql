SELECT * FROM (SELECT status, carrier, cost FROM shipments)
PIVOT (MAX(cost) FOR carrier IN ('Flash', 'FedEx', 'UPS'))
ORDER BY status;
