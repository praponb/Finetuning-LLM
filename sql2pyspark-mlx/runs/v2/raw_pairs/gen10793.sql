SELECT carrier, MAX(cost) max_cost, COUNT(cost) cost_count FROM shipments WHERE carrier != 'UPS' GROUP BY carrier HAVING max_cost < 42
