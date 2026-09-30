SELECT id, delivered_date, cost, cost - LAG(cost, 1) OVER (ORDER BY delivered_date, id) AS cost_change FROM shipments;
