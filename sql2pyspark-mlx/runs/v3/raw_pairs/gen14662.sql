SELECT sessions.country, web_events.duration_sec FROM sessions LEFT OUTER JOIN web_events ON web_events.session_id = sessions.id WHERE web_events.page = '/search';
