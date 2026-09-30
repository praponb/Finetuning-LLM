WITH ranked_sessions AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY started_at) AS rnk
    FROM sessions
)
SELECT id, customer_id, started_at
FROM ranked_sessions
WHERE rnk <= 5;
