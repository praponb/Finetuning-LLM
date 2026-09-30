SELECT id, customer_id, started_at, device FROM sessions WHERE country = 'JP' ORDER BY started_at DESC, id LIMIT 3
