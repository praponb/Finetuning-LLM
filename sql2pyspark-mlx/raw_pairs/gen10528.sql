SELECT sessions.device, ROUND((100 * SUM(CASE WHEN web_events.page = '/home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home
FROM web_events
LEFT OUTER JOIN sessions ON web_events.session_id = sessions.id
GROUP BY sessions.device;
