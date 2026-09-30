SELECT id, started_at FROM sessions WHERE device = 'mobile' AND device NOT IN ('desktop', 'tablet', 'mobile') AND customer_id IS NOT NULL ORDER BY started_at, id LIMIT 5;
