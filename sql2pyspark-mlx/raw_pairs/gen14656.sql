select we.id, we.duration_sec, s.device from web_events we inner join sessions s on we.session_id = s.id where hour(s.started_at) between 11 and 14 and s.browser in ('Edge', 'Safari', 'Chrome');
