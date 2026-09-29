select
    id,
    customer_id,
    device,
    country,
    cast(started_at as date) as started_at_day
from sessions
order by country, id
