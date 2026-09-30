select
    s.id,
    s.customer_id,
    s.browser,
    s.started_at
from sessions s
where s.browser = 'Firefox' and s.customer_id in (select id from customers where is_active)
