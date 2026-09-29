select s.browser, count(*) as total_count from web_events we join sessions s on we.session_id = s.id where we.session_id in (17, 21, 27, 33) group by s.browser order by total_count
