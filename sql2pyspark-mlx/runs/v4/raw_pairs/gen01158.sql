select s.id, s.browser, s.country, s.device from sessions s where s.device in ('desktop', 'mobile') and exists (select 1 from web_events we where we.session_id = s.id) order by s.id desc;
