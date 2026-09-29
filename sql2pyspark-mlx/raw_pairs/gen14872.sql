select s.country, we.page from sessions s left outer join web_events we on s.id = we.session_id where we.duration_sec between 190 and 436;
