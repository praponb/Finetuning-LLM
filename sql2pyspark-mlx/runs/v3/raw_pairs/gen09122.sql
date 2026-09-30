select
    id,
    started_at,
    customer_id,
    browser
from sessions
where customer_id is not null
order by started_at
