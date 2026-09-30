SELECT status, carrier, min_cost FROM (SELECT status, carrier, MIN(cost) AS min_cost FROM shipments GROUP BY status, carrier) AS s WHERE min_cost < 54;
