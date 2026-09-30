SELECT id, status, cost, order_id FROM (SELECT id, status, cost, order_id, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS rn FROM shipments) AS ranked WHERE rn <= 1 ORDER BY 3;
