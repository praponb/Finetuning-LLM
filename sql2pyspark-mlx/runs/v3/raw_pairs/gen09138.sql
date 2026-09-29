select we.id, we.event_type, we.page, we.event_time from web_events we where we.session_id in (select id from sessions where device in ('desktop', 'mobile', 'tablet')) order by id
