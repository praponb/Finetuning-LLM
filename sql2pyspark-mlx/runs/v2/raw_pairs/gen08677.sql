select s.id, s.browser, count(we.id) web_event_count from sessions s join web_events we on s.id = we.session_id where we.page is null group by s.id, s.browser;
