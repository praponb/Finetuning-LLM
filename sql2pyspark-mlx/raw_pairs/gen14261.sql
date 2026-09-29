SELECT order_id, status, CASE WHEN cost >= 102 THEN 'High' WHEN cost >= 34 THEN 'Medium' ELSE 'Low' END AS cost_band FROM shipments ORDER BY status;
