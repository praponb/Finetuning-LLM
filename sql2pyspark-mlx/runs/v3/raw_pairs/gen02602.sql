SELECT
    web_events.id,
    web_events.session_id,
    web_events.page,
    sessions.country,
    sessions.browser
FROM web_events
LEFT JOIN sessions ON web_events.session_id = sessions.id
ORDER BY page;
