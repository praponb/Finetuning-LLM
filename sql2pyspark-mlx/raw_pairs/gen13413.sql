select we.id, we.event_time, we.page
from web_events we
where we.session_id in (3, 5, 26) and we.session_id not in (select id from sessions where customer_id = 18);
