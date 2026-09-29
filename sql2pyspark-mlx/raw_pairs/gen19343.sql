SELECT browser
FROM sessions
WHERE (customer_id IN (3, 34) AND device = 'desktop') OR started_at < '2025-09-01';
