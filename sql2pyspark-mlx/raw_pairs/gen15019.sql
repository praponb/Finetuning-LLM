SELECT carrier, SUM(cost) AS total_cost FROM shipments WHERE carrier IN ('FedEx', 'DHL') OR carrier IS NOT NULL GROUP BY carrier;
