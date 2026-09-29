select
    customer_id,
    country,
    started_at,
    date_trunc('YEAR', started_at) as started_at_period
from sessions
where customer_id is null and device in ('tablet', 'mobile', 'desktop')
order by country
