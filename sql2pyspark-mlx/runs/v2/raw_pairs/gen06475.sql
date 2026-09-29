WITH top_sessions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) rnk FROM sessions) SELECT id, customer_id, started_at, browser FROM top_sessions WHERE rnk = 1;
