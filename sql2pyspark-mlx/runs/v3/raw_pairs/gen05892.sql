SELECT id, country, started_at FROM sessions WHERE device IN ('desktop', 'tablet', 'mobile') ORDER BY started_at, id;
