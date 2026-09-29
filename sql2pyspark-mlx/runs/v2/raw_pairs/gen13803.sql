SELECT * FROM (SELECT status, carrier, cost FROM shipments)
PIVOT (AVG(cost) FOR carrier IN ('DHL', 'Flash'));
