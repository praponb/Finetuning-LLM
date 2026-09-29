SELECT cost, delivered_date
FROM shipments
WHERE status IN ('IN_TRANSIT', 'DELIVERED', 'RETURNED')
