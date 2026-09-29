SELECT session_id, SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) search_duration_sec FROM web_events WHERE page IS NOT NULL GROUP BY session_id
