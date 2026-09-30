SELECT 'shipment' source, carrier label
FROM shipments
UNION
SELECT 'web_event' source, page label
FROM web_events
