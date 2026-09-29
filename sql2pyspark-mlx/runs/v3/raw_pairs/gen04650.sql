select s.customer_id, we.page, we.event_type
from sessions s
left outer join web_events we on we.session_id = s.id
