SELECT * FROM (SELECT status, carrier, cost FROM shipments WHERE shipped_date <= '2024-01-01') PIVOT (AVG(cost) FOR carrier IN ('Flash', 'Kerry', 'FedEx', 'DHL'))
