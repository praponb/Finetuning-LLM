SELECT id, carrier, delivered_date, SUBSTRING(status, 1, 2) AS status_prefix, COALESCE(cost, 0) AS cost_filled FROM shipments
