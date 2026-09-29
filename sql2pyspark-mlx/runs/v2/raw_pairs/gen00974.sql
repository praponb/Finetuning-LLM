SELECT id, carrier, status, order_id, CAST(cost AS INT) AS cost_int FROM shipments WHERE cost > 62;
