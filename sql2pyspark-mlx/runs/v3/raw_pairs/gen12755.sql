select s.country, min(we.duration_sec) as min_duration_sec from web_events we right join sessions s on s.id = we.session_id where we.duration_sec < 453 group by s.country
