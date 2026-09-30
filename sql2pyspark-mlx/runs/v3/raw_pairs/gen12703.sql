WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS rnk
    FROM web_events
)
SELECT id, session_id, duration_sec
FROM ranked
WHERE rnk <= 1;
