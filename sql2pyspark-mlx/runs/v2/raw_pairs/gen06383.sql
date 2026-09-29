WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) rn
    FROM web_events
    WHERE page IN ('/cart', '/home', '/checkout')
)
SELECT id, session_id, duration_sec
FROM Numbered
WHERE rn <= 5
