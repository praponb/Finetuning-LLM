select we.id, we.page, we.session_id, we.event_time from web_events we where we.session_id in (select id from sessions where country in ('JP', 'UK')) order by page;
