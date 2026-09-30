SELECT id, delivered_date, UPPER(status) AS status_upper
FROM shipments
WHERE status IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED');
