SELECT * FROM (SELECT status, carrier, cost FROM shipments)
PIVOT (COUNT(cost) FOR carrier IN ('Flash', 'UPS', 'Kerry', 'FedEx'))
ORDER BY status
