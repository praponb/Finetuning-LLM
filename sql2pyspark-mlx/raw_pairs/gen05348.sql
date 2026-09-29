SELECT id, delivered_date, COALESCE(carrier, 'unassigned') AS carrier_filled
FROM shipments
WHERE order_id IN (9, 14, 39) AND YEAR(delivered_date) = 2023;
