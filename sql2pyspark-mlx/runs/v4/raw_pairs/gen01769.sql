SELECT *
FROM shipments
WHERE (MONTH(shipped_date) IN (2, 3, 5) AND cost <= 58) OR carrier NOT IN ('UPS', 'Kerry')
ORDER BY shipped_date
