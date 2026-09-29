SELECT sessions.country, COUNT(DISTINCT web_events.duration_sec) AS unique_sessions
FROM web_events
FULL OUTER JOIN sessions ON web_events.session_id = sessions.id
WHERE web_events.duration_sec BETWEEN 125 AND 338
GROUP BY sessions.country
ORDER BY unique_sessions DESC;
