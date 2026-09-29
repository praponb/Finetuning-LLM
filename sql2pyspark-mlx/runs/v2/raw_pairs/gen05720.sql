SELECT id, status, delivered_date, cost, SUM(cost) OVER (PARTITION BY status ORDER BY delivered_date, id) AS running_cost FROM shipments WHERE YEAR(delivered_date) = 2023
