SELECT id, status, SUBSTRING(status, 1, 3) AS status_prefix, SUBSTRING(status, 1, 2) AS status_prefix_2 FROM shipments;
