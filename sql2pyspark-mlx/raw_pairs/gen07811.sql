select id, device
from sessions
where started_at < '2023-07-01' and (customer_id in (15, 34) or customer_id in (3, 11, 17));
