SELECT id, customer_id, started_at, LEAD(started_at, 2) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS next_started_at FROM sessions WHERE country NOT IN ('JP', 'UK')
