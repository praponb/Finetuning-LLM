SELECT id, carrier, cost, MAX(cost) OVER (PARTITION BY carrier) AS carrier_max_cost FROM shipments ORDER BY 2 DESC
