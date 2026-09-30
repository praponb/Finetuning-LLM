WITH session_totals AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.id, sessions.customer_id, session_totals.purchase_count
FROM sessions
JOIN session_totals ON sessions.id = session_totals.session_id
WHERE session_totals.purchase_count >= 80;
