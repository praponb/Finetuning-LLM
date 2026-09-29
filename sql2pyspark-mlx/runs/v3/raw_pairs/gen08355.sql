SELECT order_id, status
FROM shipments
WHERE status = 'IN_TRANSIT' AND shipped_date > '2025-03-22'
