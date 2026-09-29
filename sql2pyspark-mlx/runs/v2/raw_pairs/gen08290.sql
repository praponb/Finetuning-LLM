SELECT id, status, delivered_date, cost, LAST_VALUE(cost) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_cost FROM shipments
