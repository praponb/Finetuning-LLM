SELECT * FROM (SELECT status, carrier, cost FROM shipments)
PIVOT (COUNT(cost) FOR carrier IN ('DHL', 'FedEx'))
ORDER BY status;
