SELECT * FROM (SELECT status, carrier, cost FROM shipments) PIVOT (SUM(cost) FOR carrier IN ('Flash', 'Kerry', 'FedEx'))
