select we.session_id, s.started_at from web_events we left join sessions s on s.id = we.session_id where s.country = 'US' order by session_id desc
