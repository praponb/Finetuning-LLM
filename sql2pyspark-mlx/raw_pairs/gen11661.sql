select s.id, s.browser, count(we.id) as web_event_count from sessions s left outer join web_events we on we.session_id = s.id where s.started_at < '2025-04-01' group by s.id, s.browser
