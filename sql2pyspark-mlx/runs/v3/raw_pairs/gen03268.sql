select
    id,
    started_at,
    country,
    customer_id
from sessions
where customer_id = 5 or customer_id in (16, 32);
