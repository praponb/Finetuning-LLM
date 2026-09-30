SELECT id, carrier, cost, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) AS cost_row FROM shipments WHERE order_id IN (1, 8, 15)
