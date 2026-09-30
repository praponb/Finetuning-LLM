SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE shipped_date < '2025-08-01') PIVOT (MAX(cost) FOR carrier IN ('DHL', 'FedEx'))
