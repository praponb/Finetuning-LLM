select sessions.id, sessions.device
from sessions
where sessions.country <> 'SG' and sessions.customer_id in (select id from customers where is_active = true)
