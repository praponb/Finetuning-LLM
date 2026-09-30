SELECT * FROM (SELECT status, carrier, cost FROM shipments)
PIVOT (SUM(cost) FOR carrier IN ('DHL', 'Flash', 'FedEx', 'UPS'))
