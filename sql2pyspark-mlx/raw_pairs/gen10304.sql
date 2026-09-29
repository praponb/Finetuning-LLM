SELECT
    id,
    customer_id,
    device,
    started_at,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', started_at), customer_id ORDER BY started_at, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS row_seq
FROM sessions
WHERE device = 'tablet';
