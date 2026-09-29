SELECT status, DATE_TRUNC('week', shipped_date) shipped_date_period
FROM shipments
WHERE carrier = 'DHL'
