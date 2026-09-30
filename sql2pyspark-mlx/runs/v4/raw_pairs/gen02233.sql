WITH ranked_sessions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS rnk FROM sessions) SELECT * FROM ranked_sessions WHERE rnk <= 2 ORDER BY browser DESC
