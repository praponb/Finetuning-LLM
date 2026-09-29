select web_events.id, web_events.page from web_events where web_events.session_id not in (select id from sessions where customer_id is not null) order by web_events.id desc
