SELECT id, shipped_date, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
FROM shipments
WHERE order_id IS NULL AND (cost < 72 OR status = 'LOST')
ORDER BY shipped_date DESC
