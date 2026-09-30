SELECT carrier, COALESCE(cost, 0) AS cost_filled
FROM shipments
ORDER BY carrier;
