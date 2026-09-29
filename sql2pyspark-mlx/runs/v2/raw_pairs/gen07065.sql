SELECT carrier, DATE_TRUNC('YEAR', shipped_date) AS shipped_date_period
FROM shipments
WHERE carrier IN ('DHL', 'Flash') OR cost BETWEEN 71 AND 72
