select s.id, s.customer_id, s.started_at from sessions s where s.id in (select session_id from web_events where page = '/product') order by id desc;
