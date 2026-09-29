SELECT id, status, carrier, CASE WHEN cost >= 73 THEN 'High' WHEN cost >= 31 THEN 'Medium' ELSE 'Low' END AS cost_level FROM shipments
