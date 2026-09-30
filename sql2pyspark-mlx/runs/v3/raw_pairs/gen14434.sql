select s.id, s.country, s.device from sessions s where s.country = 'US' and exists (select 1 from web_events we where we.session_id = s.id and we.page = '/checkout');
