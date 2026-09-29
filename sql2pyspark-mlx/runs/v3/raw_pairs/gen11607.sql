SELECT *, COALESCE(cost, 0) AS cost_filled FROM shipments WHERE delivered_date <= '2025-08-01' OR carrier = 'FedEx'
