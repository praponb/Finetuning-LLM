SELECT id, carrier, cost, MIN(cost) OVER (PARTITION BY carrier) carrier_min_cost FROM shipments WHERE cost > 88;
