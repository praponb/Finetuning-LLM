SELECT MAX(cost) AS max_cost FROM shipments WHERE carrier IN ('UPS', 'Flash') AND (MONTH(delivered_date) IN (3, 7, 11) OR delivered_date < '2025-03-01');
