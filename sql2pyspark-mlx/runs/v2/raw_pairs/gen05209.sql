select
    s.id,
    s.country,
    s.device,
    s.customer_id
from sessions s
where s.country = 'US' and s.id in (select session_id from web_events);
