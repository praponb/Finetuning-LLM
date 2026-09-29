select we.id, we.event_time, s.started_at, s.customer_id, s.device, concat(s.browser, '/', s.country) as browser_country from web_events we inner join sessions s on we.session_id = s.id;
