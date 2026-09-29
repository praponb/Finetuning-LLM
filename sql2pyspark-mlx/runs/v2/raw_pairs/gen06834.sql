select we.session_id, s.country from web_events we full outer join sessions s on s.id = we.session_id order by country;
