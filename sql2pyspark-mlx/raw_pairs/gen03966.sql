SELECT MIN(shipped_date) AS first_shipped_date
FROM shipments
WHERE order_id IS NULL AND (status IN ('RETURNED', 'IN_TRANSIT') OR carrier IN ('UPS', 'Flash', 'FedEx'))
