select s.id, s.customer_id, s.browser from sessions s where exists (select 1 from web_events we where we.session_id = s.id and we.session_id in (5, 28)) order by s.browser, id limit 5;
