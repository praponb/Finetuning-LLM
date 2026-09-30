SELECT *, COALESCE(duration_sec, 0) AS duration_sec_filled, COALESCE(page, 'unassigned') AS page_filled
FROM web_events
WHERE session_id IN (9, 14, 34)
ORDER BY event_time ASC;
