SELECT carrier, SUM(CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END) AS dhl_count FROM shipments WHERE carrier IN ('UPS', 'Flash', 'FedEx') GROUP BY 1 ORDER BY carrier ASC LIMIT 5
