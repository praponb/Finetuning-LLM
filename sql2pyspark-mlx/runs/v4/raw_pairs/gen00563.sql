SELECT status, row_count FROM (SELECT status, COUNT(*) AS row_count FROM shipments GROUP BY status) AS x WHERE row_count >= 7
