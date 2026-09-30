SELECT id, session_id, CASE WHEN duration_sec >= 370 THEN 'High' WHEN duration_sec >= 126 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier, UPPER(page) AS page_upper FROM web_events
