SELECT id, started_at, customer_id FROM sessions WHERE country = 'JP' OR HOUR(started_at) BETWEEN 11 AND 19 ORDER BY customer_id DESC, id DESC LIMIT 1;
