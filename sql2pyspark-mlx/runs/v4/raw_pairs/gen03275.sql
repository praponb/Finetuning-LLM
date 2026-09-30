SELECT status, min_cost, cnt FROM (SELECT status, MIN(cost) AS min_cost, COUNT(*) AS cnt FROM shipments WHERE order_id IN (7, 21, 27, 40) GROUP BY status) AS sub WHERE min_cost > 52
