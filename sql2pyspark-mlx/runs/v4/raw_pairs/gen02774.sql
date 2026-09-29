select
    s.id,
    s.started_at,
    s.device,
    s.browser
from sessions s
where s.customer_id in (select id from customers where country not in ('JP', 'UK'));
