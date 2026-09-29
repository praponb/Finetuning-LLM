WITH ranked AS (SELECT id, order_id, cost, status, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) AS pos FROM shipments) SELECT id, order_id, cost, status FROM ranked WHERE pos <= 2;
