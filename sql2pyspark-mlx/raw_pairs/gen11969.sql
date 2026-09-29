SELECT d.channel, src.carrier, COUNT(*) AS rows_total, SUM(src.cost) AS total_cost FROM shipments src LEFT JOIN orders d ON src.order_id = d.id GROUP BY d.channel, src.carrier
