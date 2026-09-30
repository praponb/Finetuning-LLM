SELECT status, MAX(shipped_date) AS latest_shipped_date FROM shipments WHERE carrier NOT IN ('UPS', 'DHL', 'FedEx') GROUP BY status ORDER BY latest_shipped_date DESC
