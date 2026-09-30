WITH web_events_by_session AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'add_to_cart' THEN duration_sec ELSE 0 END) AS add_to_cart_duration_sec, ROUND((100 * SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cart
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    webs.add_to_cart_duration_sec,
    webs.pct_cart
FROM sessions s
LEFT JOIN web_events_by_session webs ON s.id = webs.session_id;
