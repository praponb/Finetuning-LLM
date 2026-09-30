WITH ranked_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS pos
    FROM web_events
    WHERE page IN ('/home', '/checkout', '/product')
)
SELECT *
FROM ranked_web_events
WHERE pos = 1;
