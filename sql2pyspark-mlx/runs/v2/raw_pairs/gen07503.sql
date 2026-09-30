SELECT id, carrier, status, cost, SUM(cost) OVER (PARTITION BY carrier, status) AS carrier_sum_cost FROM shipments ORDER BY status DESC;
