select we.session_id, s.device, s.customer_id
from web_events we
inner join sessions s on we.session_id = s.id
order by we.session_id desc;
