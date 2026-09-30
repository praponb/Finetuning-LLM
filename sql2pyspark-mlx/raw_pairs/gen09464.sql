SELECT id, carrier, shipped_date, COALESCE(carrier, 'N/A') AS carrier_filled FROM shipments WHERE status = 'DELIVERED'
