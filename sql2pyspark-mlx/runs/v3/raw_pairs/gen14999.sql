WITH ranked_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS rn
    FROM web_events
)
SELECT id, session_id, duration_sec
FROM ranked_web_events
WHERE rn <= 3
ORDER BY duration_sec DESC;
