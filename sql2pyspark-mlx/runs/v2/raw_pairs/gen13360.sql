select s.customer_id, s.device, we.event_type from sessions s left join web_events we on we.session_id = s.id where s.country = 'TH' order by 2;
