select
    s.id,
    s.device,
    s.browser,
    s.started_at
from sessions s
where s.customer_id in (select id from customers where signup_date > '2023-07-01')
