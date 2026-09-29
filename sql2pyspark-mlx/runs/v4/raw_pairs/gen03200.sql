SELECT id, order_id, status, cost, SUM(cost) OVER (PARTITION BY order_id, status) AS order_id_sum_cost FROM shipments WHERE delivered_date < '2024-03-01' ORDER BY status;
