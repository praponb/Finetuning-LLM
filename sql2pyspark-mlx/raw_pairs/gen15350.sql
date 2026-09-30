SELECT session_id, COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count FROM web_events GROUP BY session_id HAVING home_count > 3;
