SELECT id, delivered_date, COALESCE(carrier, 'Unknown') AS carrier_filled
FROM shipments
