SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE carrier IN ('UPS', 'Flash', 'Kerry'))
PIVOT (AVG(cost) FOR carrier IN ('DHL', 'Flash', 'FedEx', 'Kerry'))
