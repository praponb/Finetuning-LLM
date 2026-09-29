SELECT *
FROM shipments
WHERE carrier NOT IN ('UPS', 'DHL') AND carrier NOT IN ('DHL', 'UPS')
ORDER BY status DESC
