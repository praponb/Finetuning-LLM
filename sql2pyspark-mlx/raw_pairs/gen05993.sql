select s.id, s.country, count(we.id) as web_event_count from sessions s left outer join web_events we on s.id = we.session_id group by s.id, s.country having count(we.id) >= 1;
