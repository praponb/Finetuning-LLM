WITH SessionSummary AS (
    SELECT session_id, COUNT(DISTINCT duration_sec) unique_pages
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.customer_id, SessionSummary.unique_pages
FROM sessions
LEFT JOIN SessionSummary ON sessions.id = SessionSummary.session_id
WHERE SessionSummary.unique_pages > 4
ORDER BY sessions.customer_id DESC, SessionSummary.unique_pages DESC
