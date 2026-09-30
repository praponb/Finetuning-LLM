select
    we.id,
    we.session_id,
    we.event_time,
    we.page
from web_events we
where we.duration_sec > 382 and we.session_id in (select id from sessions where customer_id is not null);
