SELECT id, delivered_date, cost, LAG(cost, 1) OVER (ORDER BY delivered_date, id) AS prev_cost FROM shipments WHERE status NOT IN ('DELIVERED', 'IN_TRANSIT') ORDER BY id DESC;
