select
    sessions.id,
    sessions.started_at,
    sessions.device,
    sessions.country
from sessions
where sessions.browser in ('Chrome', 'Safari') and sessions.customer_id not in (select id from customers where signup_date > '2024-12-01')
order by country;
