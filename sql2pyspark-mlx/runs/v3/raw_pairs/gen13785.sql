SELECT sessions.browser, COUNT(web_events.id) AS web_event_count FROM sessions LEFT JOIN web_events ON web_events.session_id = sessions.id WHERE web_events.session_id = 10 GROUP BY sessions.browser;
