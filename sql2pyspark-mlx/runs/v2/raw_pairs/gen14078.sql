SELECT DISTINCT carrier, status
FROM shipments
WHERE status IN ('DELIVERED', 'RETURNED');
