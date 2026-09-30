SELECT shipped_date, COALESCE(carrier, 'unassigned') AS carrier_filled
FROM shipments
WHERE status IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED');
