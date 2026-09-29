select s.id, s.country from sessions s where exists (select 1 from web_events we where we.session_id = s.id and we.event_type in ('add_to_cart', 'search'))
