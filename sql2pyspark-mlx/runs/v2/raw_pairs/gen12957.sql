select s.browser, avg(we.duration_sec) as avg_duration_sec from web_events we left outer join sessions s on we.session_id = s.id group by s.browser order by s.browser
