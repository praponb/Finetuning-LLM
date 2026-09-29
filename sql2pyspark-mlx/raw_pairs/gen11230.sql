SELECT shipped_date, cost, delivered_date
FROM shipments
WHERE carrier IN ('UPS', 'DHL', 'FedEx')
ORDER BY shipped_date ASC;
