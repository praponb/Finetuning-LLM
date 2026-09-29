SELECT id, delivered_date, cost, LAG(cost, 2) OVER (ORDER BY delivered_date, id) AS prev_cost FROM shipments
