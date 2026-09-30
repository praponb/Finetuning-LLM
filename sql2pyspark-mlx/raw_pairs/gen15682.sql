SELECT id, started_at, device, ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', started_at), device ORDER BY started_at, id) AS started_at_row FROM sessions WHERE device = 'tablet'
