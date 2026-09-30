SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE order_id IN (1, 14, 34)) PIVOT (AVG(cost) FOR carrier IN ('Flash', 'FedEx', 'Kerry'))
