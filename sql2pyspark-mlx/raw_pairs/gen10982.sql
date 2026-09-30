SELECT status, COUNT(DISTINCT cost) AS unique_orders, COUNT(*) AS total_count FROM shipments GROUP BY status;
